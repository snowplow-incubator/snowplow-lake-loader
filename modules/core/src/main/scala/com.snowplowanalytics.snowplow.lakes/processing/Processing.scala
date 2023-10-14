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
import cats.{Applicative, Foldable, Functor, Monad}
import cats.effect.{Async, Sync}
import cats.effect.kernel.{Ref, Unique}
import fs2.{Chunk, Pipe, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.spark.sql.Row

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.lakes.Environment
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._
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

  private case class ParseResult(
    events: List[Event],
    bad: List[BadRow],
    originalBytes: Long
  )

  private case class Batched(
    events: Vector[Event], // Vector for fast concatentation when batching-up
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
    _.through(rememberTokens(ref))
      .through(incrementReceivedCount(env))
      .through(parseBytes(env, badProcessor))
      .through(handleParseFailures(env))
      .through(toBatched)
      .through(BatchUp.noTimeout(env.inMemBatchBytes))
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
        (bad, rows) <- transformToSpark[F](badProcessor, events, nonAtomicFields)
      } yield (bad, rows, SparkSchema.forBatch(nonAtomicFields.fields))

      prepare.flatMap { case (bad, rows, schema) =>
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

  private def rememberTokens[F[_]: Functor](ref: Ref[F, WindowState]): Pipe[F, TokenedEvents, Chunk[ByteBuffer]] =
    _.evalMap { case TokenedEvents(events, token, _) =>
      ref.update(state => state.copy(tokens = token :: state.tokens)).as(events)
    }

  private def incrementReceivedCount[F[_]](env: Environment[F]): Pipe[F, Chunk[ByteBuffer], Chunk[ByteBuffer]] =
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
  ): Pipe[F, Chunk[ByteBuffer], ParseResult] =
    _.parEvalMapUnordered(env.cpuParallelism) { chunk =>
      for {
        numBytes <- Sync[F].delay(Foldable[Chunk].sumBytes(chunk))
        (badRows, events) <- Foldable[Chunk].traverseSeparateUnordered(chunk) { byteBuffer =>
                               Sync[F].delay {
                                 val stringified = StandardCharsets.UTF_8.decode(byteBuffer).toString
                                 Event.parse(stringified).toEither.leftMap { failure =>
                                   val payload = BadRowRawPayload(stringified)
                                   BadRow.LoaderParsingError(processor, failure, payload)
                                 }
                               }
                             }
      } yield ParseResult(events, badRows, numBytes)
    }

  private def toBatched[F[_]: Applicative]: Pipe[F, ParseResult, Batched] =
    _.map { case ParseResult(events, _, numBytes) =>
      val entities = Foldable[List].foldMap(events)(TabledEntity.forEvent(_))
      Batched(events.toVector, entities, numBytes)
    }

  private implicit def batchable: BatchUp.Batchable[Batched] = new BatchUp.Batchable[Batched] {
    def combine(x: Batched, y: Batched): Batched =
      Batched(x.events |+| y.events, x.entities |+| y.entities, x.originalBytes + y.originalBytes)
    def weightOf(a: Batched): Long = a.originalBytes
  }

  // The pure computation is wrapped in a F to help the Cats Effect runtime to periodically cede to other fibers
  private def transformToSpark[F[_]: Sync](
    processor: BadRowProcessor,
    events: Vector[Event],
    entities: NonAtomicFields.Result
  ): F[(List[BadRow], List[Row])] =
    Foldable[Vector].traverseSeparateUnordered(events) { event =>
      Sync[F].delay {
        Transform
          .transformEvent[Any](processor, SparkCaster, event, entities)
          .map(SparkCaster.structValue(_))
      }
    }

  private def handleParseFailures[F[_]: Applicative, A](env: Environment[F]): Pipe[F, ParseResult, ParseResult] =
    _.evalTap { batch =>
      sendFailedEvents(env, batch.bad)
    }

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
