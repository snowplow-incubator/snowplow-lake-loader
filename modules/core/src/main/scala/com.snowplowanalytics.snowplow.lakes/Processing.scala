package com.snowplowanalytics.snowplow.lakes

import cats.implicits._
import cats.{Applicative, Functor, Monoid}
import cats.effect.{Async, Sync}
import cats.effect.kernel.{Ref, Unique}
import fs2.{Chunk, Pipe, Pull, Stream}

import org.apache.spark.sql.SparkSession

import java.nio.charset.StandardCharsets

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.sources.{EventProcessor, SourceConfig, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink

object Processing {

  private type Config = AnyConfig

  def stream[F[_]: Async](config: Config, env: Environment[F]): Stream[F, Nothing] = {
    implicit val lookup: RegistryLookup[F] = Http4sRegistryLookup(env.httpClient)
    val eventSourceConfig: SourceConfig = SourceConfig(SourceConfig.TimedWindows(config.windows))
    Stream.eval(SparkUtils.createTable(env.spark, config.output.good)).drain ++
      env.source.stream(eventSourceConfig, eventProcessor(config, env))
  }

  /** Model used between stages of the processing pipeline */

  private case class WindowState(
    tokens: List[Unique.Token],
    framesOnDisk: List[SparkUtils.DataFrameOnDisk],
    nonAtomicColumnNames: Set[String]
  )

  private case class Parsed(
    event: Event,
    originalBytes: Int,
    entities: Map[TabledEntity, Set[SchemaKey]] // caches the calculation
  )

  private case class Batched(events: List[Event], entities: Map[TabledEntity, Set[SchemaKey]])

  private case class BatchWithTypes(events: List[Event], entities: NonAtomicFields)

  private def eventProcessor[F[_]: Sync: RegistryLookup](config: Config, env: Environment[F]): EventProcessor[F] = { in =>
    val resources = for {
      stateRef <- Stream.eval(Ref[F].of(WindowState(Nil, Nil, Set.empty)))
      _ <- Stream.bracket(Sync[F].unit)(_ => dropViews(env.spark, stateRef))
    } yield stateRef

    resources.flatMap { stateRef =>
      in.through(processBatch(env, stateRef)) ++ finalizeWindow(config, env.spark, stateRef)
    }
  }

  private def dropViews[F[_]: Sync](spark: SparkSession, ref: Ref[F, WindowState]): F[Unit] =
    for {
      state <- ref.get
      _ <- SparkUtils.dropViews(spark, state.framesOnDisk)
    } yield ()

  private def processBatch[F[_]: Sync: RegistryLookup](
    env: Environment[F],
    ref: Ref[F, WindowState]
  ): Pipe[F, TokenedEvents, Nothing] =
    _.through(rememberTokens(ref))
      .through(parseBytes(env.processor))
      .through(sendFailedEvents(env.badSink))
      .through(batchUp(env.inMemMaxBytes))
      // We have now received a full batch of events ready for sinking to local disk
      .through(resolveTypes(env.resolver))
      .through(rememberColumnNames(ref))
      .through(transformToSpark(env.processor))
      .through(sendFailedEvents(env.badSink))
      .through(saveDataFrameToDisk(env.spark))
      .through(rememberDataFrame(ref))

  private def rememberTokens[F[_]: Functor](ref: Ref[F, WindowState]): Pipe[F, TokenedEvents, List[Array[Byte]]] =
    _.evalMap { case TokenedEvents(events, token) =>
      ref.update(state => state.copy(tokens = token :: state.tokens)).as(events)
    }

  private def rememberDataFrame[F[_]](ref: Ref[F, WindowState]): Pipe[F, SparkUtils.DataFrameOnDisk, Nothing] =
    _.evalMap { df =>
      ref.update(state => state.copy(framesOnDisk = df :: state.framesOnDisk))
    }.drain

  private def rememberColumnNames[F[_]](ref: Ref[F, WindowState]): Pipe[F, BatchWithTypes, BatchWithTypes] =
    _.evalTap { case BatchWithTypes(_, entities) =>
      val colNames = entities.fields.flatMap { colGrp =>
        colGrp.field.name :: colGrp.recoveries.values.map(_.name).toList
      }.toSet
      ref.update(state => state.copy(nonAtomicColumnNames = state.nonAtomicColumnNames ++ colNames))
    }

  // This is a pure cpu-bound task. In future we might choose to make this parallel by changing
  // _.map to _.parEvalMap. We should do this only if we observe that bigger compute instances
  // cannot make 100% use of the available cpu.
  private def parseBytes[F[_]]( // TODO: can I change F to Pure?
    processor: BadRowProcessor
  ): Pipe[F, List[Array[Byte]], (List[BadRow], List[Parsed])] =
    _.map { list =>
      list.map { bytes =>
        val stringified = new String(bytes, StandardCharsets.UTF_8)
        Event
          .parse(stringified)
          .map(event => Parsed(event, bytes.size, TabledEntity.forEvent(event)))
          .leftMap { failure =>
            val payload = BadRowRawPayload(stringified)
            BadRow.LoaderParsingError(processor, failure, payload)
          }
      }.separate
    }

  private implicit def batchedMonoid: Monoid[Batched] = new Monoid[Batched] {
    def empty: Batched = Batched(Nil, Map.empty)
    def combine(x: Batched, y: Batched): Batched = Batched(x.events |+| y.events, x.entities |+| y.entities)
  }

  private def batchUp[F[_]](maxBytes: Long): Pipe[F, List[Parsed], Batched] = {

    def go(
      source: Stream[F, List[Parsed]],
      batch: Batched,
      batchSize: Long
    ): Pull[F, Batched, Unit] =
      source.pull.uncons1.flatMap {
        case None if batchSize > 0 => Pull.output1(batch) >> Pull.done
        case None => Pull.done
        case Some((pulled, source)) =>
          def go2(
            batch: Batched,
            pulled: List[Parsed],
            batchSize: Long
          ): Pull[F, Batched, Unit] =
            pulled match {
              case Nil => go(source, batch, batchSize)
              case head :: tail =>
                val contribution = Batched(List(head.event), head.entities)
                if (batchSize > 0 && batchSize + head.originalBytes > maxBytes)
                  Pull.output1(batch) >> go2(contribution, tail, head.originalBytes.toLong)
                else
                  go2(batch |+| contribution, tail, batchSize + head.originalBytes.toLong)
            }
          go2(batch, pulled, batchSize)
      }

    source => go(source, Monoid[Batched].empty, 0).stream
  }

  private def resolveTypes[F[_]: Sync: RegistryLookup](resolver: Resolver[F]): Pipe[F, Batched, BatchWithTypes] =
    _.evalMap { case Batched(events, entities) =>
      NonAtomicFields.resolveTypes[F](resolver, entities).map(naf => BatchWithTypes(events, naf))
    }

  // This is a pure cpu-bound task. In future we might choose to make this parallel by changing
  // `events.map` to something like `events.grouped(1000).parTraverse(...)`
  // We should do this only if we observe that bigger compute instances cannot make 100% use of the
  // available cpu.
  private def transformToSpark[F[_]](
    processor: BadRowProcessor
  ): Pipe[F, BatchWithTypes, (List[BadRow], Transform.RowsWithSchema)] =
    _.map { case BatchWithTypes(events, entities) =>
      val results = events.map(Transform.eventToRow(processor, _, entities))
      val (bad, good) = results.separate
      (bad, Transform.RowsWithSchema(good, SparkSchema.build(entities.fields.map(_.field))))
    }

  private def sendFailedEvents[F[_]: Applicative, A](sink: Sink[F]): Pipe[F, (List[BadRow], A), A] =
    _.evalMap { case (bad, good) =>
      val serialized = bad.map(_.compact.getBytes(StandardCharsets.UTF_8))
      sink.sinkSimple(serialized).as(good)
    }

  /** Returns the name of the saved data frame */
  private def saveDataFrameToDisk[F[_]: Sync](spark: SparkSession): Pipe[F, Transform.RowsWithSchema, SparkUtils.DataFrameOnDisk] =
    _.evalMapFilter { case Transform.RowsWithSchema(rows, schema) =>
      SparkUtils.saveDataFrameToDisk[F](spark, rows, schema)
    }

  private def finalizeWindow[F[_]: Sync](
    config: Config,
    spark: SparkSession,
    ref: Ref[F, WindowState]
  ): Stream[F, Unique.Token] =
    Stream.eval {
      for {
        state <- ref.get
        _ <- SparkUtils.sink(spark, config.output.good, state.framesOnDisk, state.nonAtomicColumnNames)
      } yield Chunk.seq(state.tokens)
    }.unchunks

}
