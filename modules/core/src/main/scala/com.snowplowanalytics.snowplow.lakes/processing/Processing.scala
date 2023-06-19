package com.snowplowanalytics.snowplow.lakes.processing

import cats.implicits._
import cats.data.NonEmptyList
import cats.{Applicative, Functor, Monad, Monoid}
import cats.effect.{Async, Sync}
import cats.effect.kernel.{Ref, Unique}
import fs2.{Pipe, Pull, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.charset.StandardCharsets

import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.lakes.Environment

object Processing {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] =
    Stream.eval(env.lakeWriter.createTable).flatMap { _ =>
      implicit val lookup: RegistryLookup[F] = Http4sRegistryLookup(env.httpClient)
      val eventProcessingConfig: EventProcessingConfig = EventProcessingConfig(env.windowing)
      env.source.stream(eventProcessingConfig, eventProcessor(env))
    }

  /** Model used between stages of the processing pipeline */

  private case class Parsed(
    event: Event,
    originalBytes: Int,
    entities: Map[TabledEntity, Set[SchemaSubVersion]] // caches the calculation
  )

  private case class Batched(
    events: List[Event],
    entities: Map[TabledEntity, Set[SchemaSubVersion]],
    originalBytes: Long
  )

  private case class BatchWithTypes(events: List[Event], entities: NonAtomicFields)

  private def eventProcessor[F[_]: Async: RegistryLookup](
    env: Environment[F]
  ): EventProcessor[F] = { in =>
    val resources = for {
      stateRef <- Stream.eval(Ref[F].of(WindowState.empty))
      _ <- Stream.bracket(Sync[F].unit)(_ => dropViews(env, stateRef))
    } yield stateRef

    resources.flatMap { stateRef =>
      in.through(processBatch(env, stateRef)) ++ finalizeWindow(env, stateRef)
    }
  }

  private def dropViews[F[_]: Monad](env: Environment[F], ref: Ref[F, WindowState]): F[Unit] =
    ref.get.flatMap { state =>
      if (state.framesOnDisk.nonEmpty)
        env.lakeWriter.removeDataFramesFromDisk(state.framesOnDisk)
      else
        Monad[F].unit
    }

  private def processBatch[F[_]: Async: RegistryLookup](
    env: Environment[F],
    ref: Ref[F, WindowState]
  ): Pipe[F, TokenedEvents, Nothing] =
    _.through(rememberTokens(ref))
      .evalTap(_ => Async[F].cede)
      .through(parseBytes(env.processor))
      .evalTap(_ => Async[F].cede)
      .through(sendFailedEvents(env.badSink))
      .evalTap(_ => Async[F].cede)
      .through(batchUp(env.inMemMaxBytes))
      .evalTap(_ => Async[F].cede)
      // We have now received a full batch of events ready for sinking to local disk
      .through(resolveTypes(env.resolver))
      .evalTap(_ => Async[F].cede)
      .through(rememberColumnNames(ref))
      .through(transformToSpark(env.processor))
      .through(sendFailedEvents(env.badSink))
      .through(saveDataFrameToDisk(env.lakeWriter))
      .through(rememberDataFrame(ref))

  private def rememberTokens[F[_]: Functor](ref: Ref[F, WindowState]): Pipe[F, TokenedEvents, List[Array[Byte]]] =
    _.evalMap { case TokenedEvents(events, token) =>
      ref.update(state => state.copy(tokens = token :: state.tokens)).as(events)
    }

  private def rememberDataFrame[F[_]](ref: Ref[F, WindowState]): Pipe[F, DataFrameOnDisk, Nothing] =
    _.evalMap { df =>
      ref.update(state => state.copy(framesOnDisk = df :: state.framesOnDisk))
    }.drain

  private def rememberColumnNames[F[_]](ref: Ref[F, WindowState]): Pipe[F, BatchWithTypes, BatchWithTypes] =
    _.evalTap { case BatchWithTypes(_, entities) =>
      val colNames = entities.fields.flatMap { typedTabledEntity =>
        typedTabledEntity.mergedField.name :: typedTabledEntity.recoveries.values.map(_.name).toList
      }.toSet
      ref.update(state => state.copy(nonAtomicColumnNames = state.nonAtomicColumnNames ++ colNames))
    }

  // This is a pure cpu-bound task. In future we might choose to make this parallel by changing
  // _.map to _.parEvalMap. We should do this only if we observe that bigger compute instances
  // cannot make 100% use of the available cpu.
  private def parseBytes[F[_]](
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
    def empty: Batched = Batched(Nil, Map.empty, 0L)
    def combine(x: Batched, y: Batched): Batched =
      Batched(x.events |+| y.events, x.entities |+| y.entities, x.originalBytes + y.originalBytes)
  }

  private def batchUp[F[_]](maxBytes: Long): Pipe[F, List[Parsed], Batched] = {

    def go(
      source: Stream[F, List[Parsed]],
      batch: Batched
    ): Pull[F, Batched, Unit] =
      source.pull.uncons1.flatMap {
        case None if batch.originalBytes > 0 => Pull.output1(batch) >> Pull.done
        case None => Pull.done
        case Some((Nil, source)) => go(source, batch)
        case Some((pulled, source)) =>
          val combined = pulled
            .map { case Parsed(event, originalBytes, entities) =>
              Batched(List(event), entities, originalBytes.toLong)
            }
            .foldLeft(batch)(_ |+| _)

          if (combined.originalBytes > maxBytes)
            Pull.output1(combined) >> go(source, Monoid[Batched].empty)
          else
            go(source, combined)
      }

    source => go(source, Monoid[Batched].empty).stream
  }

  private def resolveTypes[F[_]: Sync: RegistryLookup](resolver: Resolver[F]): Pipe[F, Batched, BatchWithTypes] =
    _.evalMap { case Batched(events, entities, _) =>
      NonAtomicFields.resolveTypes[F](resolver, entities).map(naf => BatchWithTypes(events, naf))
    }

  // This is a pure cpu-bound task. In future we might choose to make this parallel by changing
  // `events.map` to something like `events.grouped(1000).parTraverse(...)`
  // We should do this only if we observe that bigger compute instances cannot make 100% use of the
  // available cpu.
  private def transformToSpark[F[_]](
    processor: BadRowProcessor
  ): Pipe[F, BatchWithTypes, (List[BadRow], RowsWithSchema)] =
    _.map { case BatchWithTypes(events, entities) =>
      val results = events.map(Transform.eventToRow(processor, _, entities))
      val (bad, good) = results.separate
      (bad, RowsWithSchema(good, SparkSchema.forBatch(entities.fields.map(_.mergedField))))
    }

  private def sendFailedEvents[F[_]: Applicative, A](sink: Sink[F]): Pipe[F, (List[BadRow], A), A] =
    _.evalTap { case (bad, _) =>
      if (bad.nonEmpty) {
        val serialized = bad.map(_.compact.getBytes(StandardCharsets.UTF_8))
        sink.sinkSimple(serialized)
      } else Applicative[F].unit
    }.map(_._2)

  /** Returns the name of the saved data frame */
  private def saveDataFrameToDisk[F[_]: Sync](writer: LakeWriter[F]): Pipe[F, RowsWithSchema, DataFrameOnDisk] =
    _.evalMapFilter { case RowsWithSchema(rows, schema) =>
      NonEmptyList.fromList(rows) match {
        case Some(nel) =>
          writer.saveDataFrameToDisk(nel, schema).map[Option[DataFrameOnDisk]](Some(_))
        case None =>
          Logger[F]
            .info(s"An in-memory batch yielded zero good events.  Nothing will be saved to local disk.")
            .as(Option.empty[DataFrameOnDisk])
      }
    }

  private def finalizeWindow[F[_]: Sync](
    env: Environment[F],
    ref: Ref[F, WindowState]
  ): Stream[F, Unique.Token] =
    Stream.eval(ref.get).flatMap { state =>
      val commit = NonEmptyList.fromList(state.framesOnDisk) match {
        case Some(nel) => env.lakeWriter.commit(nel, state.nonAtomicColumnNames)
        case None => Logger[F].info("A window yielded zero good events.  Nothing will be written into the lake.")
      }

      Stream.eval(commit) >> Stream.emits(state.tokens.reverse)
    }
}
