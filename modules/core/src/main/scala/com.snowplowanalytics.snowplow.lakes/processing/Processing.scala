/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.processing

import cats.implicits._
import cats.effect.implicits._
import cats.data.NonEmptyList
import cats.{Applicative, Functor, Monad, Monoid}
import cats.effect.{Async, Sync}
import cats.effect.kernel.{Ref, Unique}
import fs2.{Pipe, Pull, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import scala.concurrent.duration.{DurationLong, FiniteDuration}

import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.lakes.{Environment, Metrics}
import com.snowplowanalytics.snowplow.loaders.transform.{NonAtomicFields, SchemaSubVersion, TabledEntity, Transform, TypedTabledEntity}

object Processing {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] =
    Stream.eval(env.lakeWriter.createTable).flatMap { _ =>
      implicit val lookup: RegistryLookup[F]           = Http4sRegistryLookup(env.httpClient)
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

  private def eventProcessor[F[_]: Async: RegistryLookup](
    env: Environment[F]
  ): EventProcessor[F] = { in =>
    val resources = for {
      now <- Stream.eval(Sync[F].realTime)
      stateRef <- Stream.eval(Ref[F].of(WindowState.empty))
      _ <- Stream.bracket(Sync[F].unit)(_ => dropViews(env, stateRef))
    } yield (stateRef, now)

    val badProcessor = BadRowProcessor(env.appInfo.name, env.appInfo.version)

    resources.flatMap { case (stateRef, realTimeWindowStarted) =>
      in.through(processBatches(env, badProcessor, stateRef))
        .append(finalizeWindow(env, stateRef, realTimeWindowStarted))
    }
  }

  private def dropViews[F[_]: Monad](env: Environment[F], ref: Ref[F, WindowState]): F[Unit] =
    ref.get.flatMap { state =>
      if (state.framesOnDisk.nonEmpty)
        env.lakeWriter.removeDataFramesFromDisk(state.framesOnDisk)
      else
        Monad[F].unit
    }

  private def processBatches[F[_]: Async: RegistryLookup](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    ref: Ref[F, WindowState]
  ): Pipe[F, TokenedEvents, Nothing] =
    _.through(setLatency(env.metrics))
      .through(rememberTokens(ref))
      .through(incrementReceivedCount(env))
      .through(parseBytes(env, badProcessor))
      .through(sendAndDropFailedEvents(env))
      .through(batchUp(env.inMemBatchBytes))
      .through(sinkParsedBatch(env, badProcessor, ref))

  /**
   * The block inside this `parEvalMapUnordered` is CPU-intensive and mainly synchronous, i.e. it
   * fully occupies a single thread. It does not help overall throughput to break this block into
   * smaller parallelizable sub-tasks. The currency level on the `parEvalMap` is an important
   * parameter for letting this loader make full use of the available CPU.
   */
  private def sinkParsedBatch[F[_]: RegistryLookup: Async](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    ref: Ref[F, WindowState]
  ): Pipe[F, Batched, Nothing] =
    _.parEvalMapUnordered(env.cpuParallelism) { case Batched(events, entities, _) =>
      val prepare = for {
        _ <- Logger[F].debug(s"Processing batch of size ${events.size}")
        nonAtomicFields <- NonAtomicFields.resolveTypes[F](env.resolver, entities)
        _ <- rememberColumnNames(ref, nonAtomicFields.fields)
        (bad, rowsWithSchema) <- transformToSpark[F](badProcessor, events, nonAtomicFields)
      } yield (bad, rowsWithSchema)

      prepare.flatMap { case (bad, RowsWithSchema(rows, schema)) =>
        val sink = NonEmptyList.fromList(rows) match {
          case Some(nel) =>
            for {
              dfOnDisk <- env.lakeWriter.saveDataFrameToDisk(nel, schema)
              _ <- rememberDataFrame(ref, dfOnDisk)
            } yield ()
          case None =>
            Logger[F].info(s"An in-memory batch yielded zero good events.  Nothing will be saved to local disk.")
        }

        sink
          .both(sendFailedEvents(env, bad))
          .flatTap { _ =>
            Logger[F].debug(s"Finished processing batch of size ${events.size}")
          }
      }
    }.drain

  private def setLatency[F[_]: Sync](metrics: Metrics[F]): Pipe[F, TokenedEvents, TokenedEvents] =
    _.evalTap {
      _.earliestSourceTstamp match {
        case Some(t) =>
          for {
            now <- Sync[F].realTime
            latency = now - t.toEpochMilli.millis
            _ <- metrics.setLatency(latency)
          } yield ()
        case None =>
          Applicative[F].unit
      }
    }

  private def rememberTokens[F[_]: Functor](ref: Ref[F, WindowState]): Pipe[F, TokenedEvents, List[ByteBuffer]] =
    _.evalMap { case TokenedEvents(events, token, _) =>
      ref.update(state => state.copy(tokens = token :: state.tokens)).as(events)
    }

  private def incrementReceivedCount[F[_]](env: Environment[F]): Pipe[F, List[ByteBuffer], List[ByteBuffer]] =
    _.evalTap { events =>
      env.metrics.addReceived(events.size)
    }

  private def rememberDataFrame[F[_]](ref: Ref[F, WindowState], dfOnDisk: DataFrameOnDisk): F[Unit] =
    ref.update(state => state.copy(framesOnDisk = dfOnDisk :: state.framesOnDisk))

  private def rememberColumnNames[F[_]](ref: Ref[F, WindowState], fields: List[TypedTabledEntity]): F[Unit] = {
    val colNames = fields.flatMap { typedTabledEntity =>
      typedTabledEntity.mergedField.name :: typedTabledEntity.recoveries.map(_._2.name)
    }.toSet
    ref.update(state => state.copy(nonAtomicColumnNames = state.nonAtomicColumnNames ++ colNames))
  }

  private def parseBytes[F[_]: Async](
    env: Environment[F],
    processor: BadRowProcessor
  ): Pipe[F, List[ByteBuffer], (List[BadRow], Batched)] =
    _.parEvalMapUnordered(env.cpuParallelism) { list =>
      list
        .traverse { bytes =>
          for {
            // order of these byte buffer operations is important
            numBytes <- Sync[F].delay(bytes.limit() - bytes.position())
            stringified <- Sync[F].delay(StandardCharsets.UTF_8.decode(bytes).toString)
          } yield Event
            .parse(stringified)
            .map(event => Parsed(event, numBytes, TabledEntity.forEvent(event)))
            .leftMap { failure =>
              val payload = BadRowRawPayload(stringified)
              BadRow.LoaderParsingError(processor, failure, payload)
            }
        }
        .map(_.separate)
        .map { case (bad, parsed) =>
          val batched = parsed.foldLeft(Monoid[Batched].empty) {
            case (Batched(allEvents, allEntities, allBytes), Parsed(event, numBytes, entities)) =>
              Batched(event :: allEvents, allEntities |+| entities, allBytes + numBytes.toLong)
          }
          (bad, batched)
        }
    }

  private implicit def batchedMonoid: Monoid[Batched] = new Monoid[Batched] {
    def empty: Batched = Batched(Nil, Map.empty, 0L)
    def combine(x: Batched, y: Batched): Batched =
      Batched(x.events |+| y.events, x.entities |+| y.entities, x.originalBytes + y.originalBytes)
  }

  private def batchUp[F[_]](maxBytes: Long): Pipe[F, Batched, Batched] = {

    def go(
      source: Stream[F, Batched],
      batch: Batched
    ): Pull[F, Batched, Unit] =
      source.pull.uncons1.flatMap {
        case None if batch.originalBytes > 0                      => Pull.output1(batch) >> Pull.done
        case None                                                 => Pull.done
        case Some((pulled, source)) if pulled.originalBytes === 0 => go(source, batch)
        case Some((pulled, source)) =>
          val combined = batch |+| pulled
          if (combined.originalBytes > maxBytes)
            Pull.output1(combined) >> go(source, Monoid[Batched].empty)
          else
            go(source, combined)
      }

    source => go(source, Monoid[Batched].empty).stream
  }

  // This is a pure cpu-bound task. In future we might choose to make this parallel by changing
  // `events.traverse` to something like `events.grouped(1000).parTraverse(...)`
  // We should do this only if we observe that bigger compute instances cannot make 100% use of the
  // available cpu.
  //
  // The computation is wrapped in Applicative[F].pure() so the Cats Effect runtime can cede to other fibers
  private def transformToSpark[F[_]: Applicative](
    processor: BadRowProcessor,
    events: List[Event],
    entities: NonAtomicFields.Result
  ): F[(List[BadRow], RowsWithSchema)] =
    events
      .traverse { event =>
        Applicative[F].pure {
          Transform
            .transformEvent[Any](processor, SparkCaster, event, entities)
            .map(SparkCaster.structValue(_))
        }
      }
      .map { results =>
        val (bad, good) = results.separate
        (bad, RowsWithSchema(good, SparkSchema.forBatch(entities.fields)))
      }

  private def sendAndDropFailedEvents[F[_]: Applicative, A](env: Environment[F]): Pipe[F, (List[BadRow], A), A] =
    _.evalTap { case (bad, _) =>
      sendFailedEvents(env, bad)
    }.map(_._2)

  private def sendFailedEvents[F[_]: Applicative, A](env: Environment[F], bad: List[BadRow]): F[Unit] =
    if (bad.nonEmpty) {
      val serialized = bad.map(_.compact.getBytes(StandardCharsets.UTF_8))
      env.metrics.addBad(bad.size) *>
        env.badSink.sinkSimple(serialized)
    } else Applicative[F].unit

  private def finalizeWindow[F[_]: Sync](
    env: Environment[F],
    ref: Ref[F, WindowState],
    realTimeWindowStarted: FiniteDuration
  ): Stream[F, Unique.Token] =
    Stream.eval(ref.get).flatMap { state =>
      val commit = NonEmptyList.fromList(state.framesOnDisk) match {
        case Some(nel) =>
          val eventCount = state.framesOnDisk.map(_.count).sum
          for {
            _ <- Logger[F].info(s"Ready to Write and commit $eventCount events to the lake.")
            _ <- Logger[F].info(s"Non atomic columns: [${state.nonAtomicColumnNames.toSeq.sorted.mkString(",")}]")
            _ <- env.lakeWriter.commit(nel)
            now <- Sync[F].realTime
            _ <- Logger[F].info(s"Finished writing and committing $eventCount events to the lake.")
            _ <- env.metrics.addCommitted(eventCount)
            _ <- env.metrics.setProcessingLatency(now - realTimeWindowStarted)
          } yield ()
        case None =>
          Logger[F].info("A window yielded zero good events.  Nothing will be written into the lake.")
      }

      Stream.eval(commit) >> Stream.emits(state.tokens.reverse)
    }
}
