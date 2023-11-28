/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}
import org.http4s.client.Client
import fs2.Stream

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.lakes.processing.{DataFrameOnDisk, LakeWriter}

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  /** All tests can use the same window duration */
  val WindowDuration = 42.seconds

  sealed trait Action
  object Action {
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Int) extends Action
    case object CreatedTable extends Action
    case class SavedDataFrameToDisk(df: DataFrameOnDisk) extends Action
    case class RemovedDataFramesFromDisk(names: List[String]) extends Action
    case class CommittedToTheLake(names: List[String]) extends Action

    /* Metrics */
    case class AddedReceivedCountMetric(count: Int) extends Action
    case class AddedBadCountMetric(count: Int) extends Action
    case class AddedCommittedCountMetric(count: Int) extends Action
    case class SetLatencyMetric(latency: FiniteDuration) extends Action
    case class SetProcessingLatencyMetric(latency: FiniteDuration) extends Action
  }
  import Action._

  /**
   * Build a mock environment for testing
   *
   * @param windows
   *   Input events to send into the environment. The outer List represents a timed window of
   *   events; the inner list represents batches of events within the window
   * @return
   *   An environment and a Ref that records the actions make by the environment
   */
  def build(windows: List[List[TokenedEvents]]): IO[MockEnvironment] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      counter <- Ref[IO].of(0)
    } yield {
      val env = Environment(
        appInfo         = TestSparkEnvironment.appInfo,
        source          = testSourceAndAck(windows, state),
        badSink         = testSink(state),
        resolver        = Resolver[IO](Nil, None),
        httpClient      = testHttpClient,
        lakeWriter      = testLakeWriter(state, counter),
        metrics         = testMetrics(state),
        inMemBatchBytes = 1000000L,
        cpuParallelism  = 1,
        windowing       = EventProcessingConfig.TimedWindows(1.minute, 1.0)
      )
      MockEnvironment(state, env)
    }

  private def testLakeWriter(state: Ref[IO, Vector[Action]], counter: Ref[IO, Int]): LakeWriter[IO] = new LakeWriter[IO] {
    def createTable: IO[Unit] =
      state.update(_ :+ CreatedTable)

    def saveDataFrameToDisk(rows: NonEmptyList[Row], schema: StructType): IO[DataFrameOnDisk] =
      for {
        next <- counter.getAndUpdate(_ + 1)
        df = DataFrameOnDisk(s"view-$next", rows.size)
        _ <- state.update(_ :+ SavedDataFrameToDisk(df))
      } yield df

    def removeDataFramesFromDisk(dataFramesOnDisk: List[DataFrameOnDisk]): IO[Unit] =
      state.update(_ :+ RemovedDataFramesFromDisk(dataFramesOnDisk.map(_.viewName)))

    def commit(dataFramesOnDisk: NonEmptyList[DataFrameOnDisk]): IO[Unit] =
      state.update(_ :+ CommittedToTheLake(dataFramesOnDisk.toList.map(_.viewName)))
  }

  private def testSourceAndAck(windows: List[List[TokenedEvents]], state: Ref[IO, Vector[Action]]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig, processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream.emits(windows).flatMap { batches =>
          Stream
            .emits(batches)
            .onFinalize(IO.sleep(WindowDuration))
            .through(processor)
            .chunks
            .evalMap { chunk =>
              state.update(_ :+ Checkpointed(chunk.toList))
            }
            .drain
        }

      def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)
    }

  private def testSink(ref: Ref[IO, Vector[Action]]): Sink[IO] = Sink[IO] { batch =>
    ref.update(_ :+ SentToBad(batch.asIterable.size))
  }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  def testMetrics(ref: Ref[IO, Vector[Action]]): Metrics[IO] = new Metrics[IO] {
    def addReceived(count: Int): IO[Unit] =
      ref.update(_ :+ AddedReceivedCountMetric(count))

    def addBad(count: Int): IO[Unit] =
      ref.update(_ :+ AddedBadCountMetric(count))

    def addCommitted(count: Int): IO[Unit] =
      ref.update(_ :+ AddedCommittedCountMetric(count))

    def setLatency(latency: FiniteDuration): IO[Unit] =
      ref.update(_ :+ SetLatencyMetric(latency))

    def setProcessingLatency(latency: FiniteDuration): IO[Unit] =
      ref.update(_ :+ SetProcessingLatencyMetric(latency))

    def report: Stream[IO, Nothing] = Stream.never[IO]
  }
}
