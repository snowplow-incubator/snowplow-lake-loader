/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.processing

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.specs2.CatsEffect
import io.circe.Json
import fs2.{Chunk, Stream}
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable

import scala.concurrent.duration.DurationInt
import java.nio.charset.StandardCharsets

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, SnowplowEvent}
import com.snowplowanalytics.snowplow.lakes.TestSparkEnvironment
import com.snowplowanalytics.snowplow.sources.TokenedEvents

class ProcessingSparkSpec extends Specification with CatsEffect {
  import ProcessingSparkSpec._

  override val Timeout = 60.seconds

  def is = sequential ^ s2"""
  The lake loader should:
    Write a single window to a delta table $e1
    Successfully write parquet file when there is an invalid schema evolution $e2
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
          val tbl  = DeltaTable.forPath(spark, tmpDir.resolve("events").toString)
          val df   = tbl.toDF
          val cols = df.columns.toSeq

          val inputEventIds  = inputEvents.flatten.map(_.event_id.toString)
          val outputEventIds = df.select("event_id").as[String].collect().toSeq
          val loadTstamps    = df.select("load_tstamp").as[java.sql.Timestamp].collect().toSeq
          val trTotals       = df.select("tr_total").as[BigDecimal].collect().toSeq

          List[MatchResult[Any]](
            cols must contain("event_id"),
            cols must contain("load_tstamp"),
            cols must contain("unstruct_event_myvendor_goodschema_7"),
            df.count() must beEqualTo(4L),
            outputEventIds must containTheSameElementsAs(inputEventIds),
            loadTstamps.toSet must haveSize(1), // single timestamp for entire window
            loadTstamps.head must not beNull,
            trTotals must contain(BigDecimal(1.23))
          ).reduce(_ and _)
        }
      }
    }
  }

  def e2 = {

    val resources = for {
      inputs <- Resource.eval(generateEventsBadEvolution.take(2).compile.toList)
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
          val tbl  = DeltaTable.forPath(spark, tmpDir.resolve("events").toString)
          val df   = tbl.toDF
          val cols = df.columns.toSeq

          val inputEventIds  = inputEvents.flatten.map(_.event_id.toString)
          val outputEventIds = df.select("event_id").as[String].collect().toSeq
          val loadTstamps    = df.select("load_tstamp").as[java.sql.Timestamp].collect().toSeq

          List[MatchResult[Any]](
            cols must contain("event_id"),
            cols must contain("load_tstamp"),
            cols must contain("unstruct_event_myvendor_badevolution_1"),
            cols must contain("unstruct_event_myvendor_badevolution_1_recovered_1_0_1_37fd804e"),
            df.count() must beEqualTo(4L),
            outputEventIds must containTheSameElementsAs(inputEventIds),
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
        val event1 = Event
          .minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0")
          .copy(tr_total = Some(1.23))
          .copy(unstruct_event = ueGood700)
        val event2 = Event
          .minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
          .copy(unstruct_event = ueGood701)
        val serialized = Chunk(event1, event2).map { e =>
          StandardCharsets.UTF_8.encode(e.toTsv)
        }
        (TokenedEvents(serialized, ack, None), List(event1, event2))
      }
    }.repeat

  def generateEventsBadEvolution: Stream[IO, (TokenedEvents, List[Event])] =
    Stream.eval {
      for {
        ack <- IO.unique
        eventId1 <- IO.randomUUID
        eventId2 <- IO.randomUUID
        collectorTstamp <- IO.realTimeInstant
      } yield {
        val event1 = Event
          .minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0")
          .copy(unstruct_event = ueBadEvolution100)
        val event2 = Event
          .minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
          .copy(unstruct_event = ueBadEvolution101)
        val serialized = Chunk(event1, event2).map { e =>
          StandardCharsets.UTF_8.encode(e.toTsv)
        }
        (TokenedEvents(serialized, ack, None), List(event1, event2))
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

  /** Some unstructured events * */

  val ueGood700 = SnowplowEvent.UnstructEvent(
    Some(
      SelfDescribingData(
        SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 0)),
        Json.obj(
          "col_a" -> Json.fromString("xyz")
        )
      )
    )
  )

  val ueGood701 = SnowplowEvent.UnstructEvent(
    Some(
      SelfDescribingData(
        SchemaKey("myvendor", "goodschema", "jsonschema", SchemaVer.Full(7, 0, 1)),
        Json.obj(
          "col_a" -> Json.fromString("xyz"),
          "col_b" -> Json.fromString("abc")
        )
      )
    )
  )

  val ueBadEvolution100 = SnowplowEvent.UnstructEvent(
    Some(
      SelfDescribingData(
        SchemaKey("myvendor", "badevolution", "jsonschema", SchemaVer.Full(1, 0, 0)),
        Json.obj(
          "col_a" -> Json.fromString("xyz")
        )
      )
    )
  )

  val ueBadEvolution101 = SnowplowEvent.UnstructEvent(
    Some(
      SelfDescribingData(
        SchemaKey("myvendor", "badevolution", "jsonschema", SchemaVer.Full(1, 0, 1)),
        Json.obj(
          "col_a" -> Json.fromInt(123)
        )
      )
    )
  )

}
