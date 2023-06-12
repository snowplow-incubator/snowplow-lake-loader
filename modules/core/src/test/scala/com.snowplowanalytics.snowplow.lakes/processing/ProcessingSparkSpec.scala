package com.snowplowanalytics.snowplow.lakes.processing

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.specs2.CatsEffect
import fs2.Stream
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable

import scala.concurrent.duration.DurationInt
import java.nio.charset.StandardCharsets

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.lakes.TestSparkEnvironment
import com.snowplowanalytics.snowplow.sources.TokenedEvents

class ProcessingSparkSpec extends Specification with CatsEffect {
  import ProcessingSparkSpec._

  override val Timeout = 60.seconds

  def is = sequential ^ s2"""
  The lake loader should:
    Write a single window to a delta table $e1
  """

  def e1 = {

    val resources = for {
      inputs <- Resource.eval(generateEvents.take(2).compile.toList)
      env <- TestSparkEnvironment.build(List(inputs.map(_._1)))
    } yield (inputs.map(_._2), env)

    val result = resources.use { case (inputEvents, env) =>
      Processing
        .stream(env.environment)
        .compile
        .drain
        .as((inputEvents, env.tmpDir))
    }

    result.flatMap { case (inputEvents, tmpDir) =>
      sparkForAssertions.use { spark =>
        IO.delay {
          import spark.implicits._
          val tbl = DeltaTable.forPath(spark, tmpDir.resolve("events").toString)
          val df = tbl.toDF
          val cols = df.columns.toSeq

          val inputEventIds = inputEvents.flatten.map(_.event_id.toString)
          val outputEventIds = df.select("event_id").as[String].collect().toSeq
          val loadTstamps = df.select("load_tstamp").as[java.sql.Timestamp].collect().toSeq

          List[MatchResult[Any]](
            cols must contain("event_id"),
            cols must contain("load_tstamp"),
            df.count() must beEqualTo(4L),
            outputEventIds.toSeq must containTheSameElementsAs(inputEventIds),
            loadTstamps.toSet must haveSize(1), // single timestamp for entire window
            loadTstamps.head must not beNull
          ).reduce(_ and _)
        }
      }
    }
  }
}

object ProcessingSparkSpec {

  def generateEvents: Stream[IO, (TokenedEvents, List[Event])] =
    Stream.eval {
      for {
        ack <- IO.unique
        eventId1 <- IO.randomUUID
        eventId2 <- IO.randomUUID
        collectorTstamp <- IO.realTimeInstant
      } yield {
        val event1 = Event.minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0")
        val event2 = Event.minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
        val serialized = List(event1, event2).map { e =>
          e.toTsv.getBytes(StandardCharsets.UTF_8)
        }
        (TokenedEvents(serialized, ack), List(event1, event2))
      }
    }.repeat

  /** A spark session just used for making assertions, not for running the code under test */
  def sparkForAssertions: Resource[IO, SparkSession] = {
    val io = IO.blocking {
      SparkSession
        .builder()
        .appName("testing")
        .master(s"local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    }
    Resource.make(io)(s => IO.blocking(s.close()))
  }

}
