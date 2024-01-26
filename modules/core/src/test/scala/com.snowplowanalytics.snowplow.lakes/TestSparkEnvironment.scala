/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import cats.effect.IO
import cats.effect.kernel.Resource
import org.http4s.client.Client
import fs2.Stream

import java.nio.file.{Files, Path}
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.lakes.processing.LakeWriter
import com.snowplowanalytics.snowplow.loaders.AppInfo

case class TestSparkEnvironment(
  environment: Environment[IO],
  tmpDir: Path
)

object TestSparkEnvironment {

  def build(windows: List[List[TokenedEvents]]): Resource[IO, TestSparkEnvironment] = for {
    tmpDir <- Resource.eval(IO.blocking(Files.createTempDirectory("lake-loader")))
    lakeWriter <- LakeWriter.build[IO](TestConfig.defaults.spark, targetConfig(tmpDir))
  } yield {
    val env = Environment(
      appInfo = appInfo,
      source = testSourceAndAck(windows),
      badSink = Sink[IO](_ => IO.unit),
      resolver = Resolver[IO](Nil, None),
      httpClient = testHttpClient,
      lakeWriter = lakeWriter,
      metrics = testMetrics,
      inMemBatchBytes = 1000000L,
      cpuParallelism = 1,
      windowing = EventProcessingConfig.TimedWindows(1.minute, 1.0)
    )

    TestSparkEnvironment(env, tmpDir)
  }

  private def testSourceAndAck(windows: List[List[TokenedEvents]]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig, processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream.emits(windows).flatMap { batches =>
          Stream
            .emits(batches)
            .through(processor)
            .drain
        }
    }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  private def targetConfig(tmp: Path) = Config.Delta(tmp.resolve("events").toUri, List("load_tstamp", "collector_tstamp"))

  val appInfo = new AppInfo {
    def name = "lake-loader-test"
    def version = "0.0.0"
    def dockerAlias = "snowplow/lake-loader-test:0.0.0"
    def cloud = "OnPrem"
  }

  def testMetrics: Metrics[IO] = new Metrics[IO] {
    def addReceived(count: Int): IO[Unit] = IO.unit
    def addBad(count: Int): IO[Unit] = IO.unit
    def addCommitted(count: Int): IO[Unit] = IO.unit
    def setProcessingLatency(latency: FiniteDuration): IO[Unit] = IO.unit
    def report: Stream[IO, Nothing] = Stream.never[IO]
  }

}
