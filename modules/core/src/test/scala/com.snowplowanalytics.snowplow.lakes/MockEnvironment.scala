package com.snowplowanalytics.snowplow.lakes

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}
import org.http4s.client.Client
import fs2.Stream

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.lakes.processing.{DataFrameOnDisk, LakeWriter}

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Int) extends Action
    case object CreatedTable extends Action
    case class SavedDataFrameToDisk(df: DataFrameOnDisk) extends Action
    case class RemovedDataFramesFromDisk(names: List[String]) extends Action
    case class CommittedToTheLake(names: List[String]) extends Action
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
        processor = BadRowProcessor("lake-loader-test", "0.0.0"),
        source = testSourceAndAck(windows, state),
        badSink = testSink(state),
        resolver = Resolver[IO](Nil, None),
        httpClient = testHttpClient,
        lakeWriter = testLakeWriter(state, counter),
        inMemMaxBytes = 1000000L,
        windowing = EventProcessingConfig.TimedWindows(1.minute, 1.0)
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

    def commit(dataFramesOnDisk: NonEmptyList[DataFrameOnDisk], nonAtomicFieldNames: Set[String]): IO[Unit] =
      state.update(_ :+ CommittedToTheLake(dataFramesOnDisk.toList.map(_.viewName)))
  }

  private def testSourceAndAck(windows: List[List[TokenedEvents]], state: Ref[IO, Vector[Action]]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig, processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream.emits(windows).flatMap { batches =>
          Stream
            .emits(batches)
            .through(processor)
            .chunks
            .evalMap { chunk =>
              state.update(_ :+ Checkpointed(chunk.toList))
            }
            .drain
        }
    }

  private def testSink(ref: Ref[IO, Vector[Action]]): Sink[IO] = Sink[IO] { batch =>
    ref.update(_ :+ SentToBad(batch.size))
  }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }
}
