package com.snowplowanalytics.snowplow.lakes

import cats.effect.IO
import cats.effect.kernel.Resource
import org.http4s.client.Client
import fs2.Stream

import java.nio.file.{Files, Path}
import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.lakes.processing.LakeWriter

case class TestSparkEnvironment(
  environment: Environment[IO],
  tmpDir: Path
)

object TestSparkEnvironment {

  def build(windows: List[List[TokenedEvents]]): Resource[IO, TestSparkEnvironment] = for {
    tmpDir <- Resource.eval(IO.blocking(Files.createTempDirectory("lake-loader")))
    lakeWriter <- LakeWriter.build[IO](testSparkConfig(tmpDir), targetConfig(tmpDir))
  } yield {
    val env = Environment(
      processor = BadRowProcessor("lake-loader-test", "0.0.0"),
      source = testSourceAndAck(windows),
      badSink = Sink[IO](_ => IO.unit),
      resolver = Resolver[IO](Nil, None),
      httpClient = testHttpClient,
      lakeWriter = lakeWriter,
      inMemMaxBytes = 1000000L,
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

  private def testSparkConfig(tmp: Path) = Config.Spark(
    localDir = tmp.resolve("local").toString,
    retries = 1,
    targetParquetSizeMB = 1000,
    threads = 1,
    conf = Map.empty
  )

  private def targetConfig(tmp: Path) = Config.Delta(tmp.resolve("events").toUri)

}
