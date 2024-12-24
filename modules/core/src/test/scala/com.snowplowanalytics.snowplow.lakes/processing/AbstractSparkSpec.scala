/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.processing

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits._
import cats.effect.testing.specs2.CatsEffect
import io.circe.Json
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration.DurationInt
import fs2.io.file.{Files, Path}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.lakes.{TestConfig, TestSparkEnvironment}

/** Base Spec for testing different output formats of this loader */
abstract class AbstractSparkSpec extends Specification with CatsEffect {
  import AbstractSparkSpec._

  override val Timeout = 60.seconds

  def is = sequential ^ s2"""
  The lake loader should:
    Write a single window of events into a lake table $e1
    Create unstruct_* column for unstructured events with valid schemas $e2
    Create recovery columns for unstructured events when schema evolution rules are broken $e3
    Not create a unstruct_event column for a schema with no fields and additionalProperties false $e4
    Create a unstruct_event column for a schema with no fields and additionalProperties true $e5
    Not create a contexts column for a schema with no fields and additionalProperties false $e6
    Create a contexts column for a schema with no fields and additionalProperties true $e7
  """

  /* Abstract definitions */

  /** Reads the table back into memory, so we can make assertions on the app's output */
  def readTable(spark: SparkSession, tmpDir: Path): DataFrame

  /** Spark config used only while reading table back into memory for assertions */
  def sparkConfig(tmpDir: Path): Map[String, String]

  def target: TestConfig.Target

  /* The specs */

  def e1 = Files[IO].tempDirectory.use { tmpDir =>
    val resources = for {
      inputs <- Resource.eval(EventUtils.inputEvents(2, EventUtils.good()))
      tokened <- Resource.eval(inputs.traverse(_.tokened))
      env <- TestSparkEnvironment.build(target, tmpDir, List(tokened))
    } yield (inputs, env)

    val result = resources.use { case (inputEvents, env) =>
      Processing
        .stream(env)
        .compile
        .drain
        .as(inputEvents)
    }

    result.flatMap { inputEvents =>
      sparkForAssertions(sparkConfig(tmpDir)).use { spark =>
        IO.blocking(readTable(spark, tmpDir)).map { df =>
          import spark.implicits._
          val cols = df.columns.toSeq

          val inputEventIds  = inputEvents.flatMap(_.value).map(_.event_id.toString)
          val outputEventIds = df.select("event_id").as[String].collect().toSeq
          val loadTstamps    = df.select("load_tstamp").as[java.sql.Timestamp].collect().toSeq
          val trTotals       = df.select("tr_total").as[BigDecimal].collect().toSeq

          List[MatchResult[Any]](
            cols must contain("event_id"),
            cols must contain("load_tstamp"),
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

  def e2 = Files[IO].tempDirectory.use { tmpDir =>
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

    val resources = for {
      inputs1 <- Resource.eval(EventUtils.inputEvents(2, EventUtils.good(ue = ueGood700)))
      tokened1 <- Resource.eval(inputs1.traverse(_.tokened))
      inputs2 <- Resource.eval(EventUtils.inputEvents(2, EventUtils.good(ue = ueGood701)))
      tokened2 <- Resource.eval(inputs2.traverse(_.tokened))
      env <- TestSparkEnvironment.build(target, tmpDir, List(tokened1, tokened2))
    } yield env

    val io = resources.use { env =>
      Processing
        .stream(env)
        .compile
        .drain
    }

    io *> {
      sparkForAssertions(sparkConfig(tmpDir)).use { spark =>
        IO.blocking(readTable(spark, tmpDir)).map { df =>
          import spark.implicits._
          val cols    = df.columns.toSeq
          val fieldAs = df.select("unstruct_event_myvendor_goodschema_7.col_a").as[String].collect().toSeq
          val fieldBs = df.select("unstruct_event_myvendor_goodschema_7.col_b").as[String].collect().toSeq

          List[MatchResult[Any]](
            cols must contain("unstruct_event_myvendor_goodschema_7"),
            fieldAs must contain("xyz"),
            fieldBs must contain("abc"),
            df.count() must beEqualTo(8L)
          ).reduce(_ and _)
        }
      }
    }
  }

  def e3 = Files[IO].tempDirectory.use { tmpDir =>
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

    val resources = for {
      inputs1 <- Resource.eval(EventUtils.inputEvents(2, EventUtils.good(ue = ueBadEvolution100)))
      tokened1 <- Resource.eval(inputs1.traverse(_.tokened))
      inputs2 <- Resource.eval(EventUtils.inputEvents(2, EventUtils.good(ue = ueBadEvolution101)))
      tokened2 <- Resource.eval(inputs2.traverse(_.tokened))
      env <- TestSparkEnvironment.build(target, tmpDir, List(tokened1, tokened2))
    } yield env

    val io = resources.use { env =>
      Processing
        .stream(env)
        .compile
        .drain
    }

    io *> {
      sparkForAssertions(sparkConfig(tmpDir)).use { spark =>
        IO.blocking(readTable(spark, tmpDir)).map { df =>
          import spark.implicits._
          val cols    = df.columns.toSeq
          val fieldAs = df.select("unstruct_event_myvendor_badevolution_1.col_a").as[String].collect().toSeq
          val recoveredAs =
            df.select("unstruct_event_myvendor_badevolution_1_recovered_1_0_1_37fd804e.col_a").as[Option[Long]].collect().toSeq

          List[MatchResult[Any]](
            cols must contain("unstruct_event_myvendor_badevolution_1"),
            cols must contain("unstruct_event_myvendor_badevolution_1_recovered_1_0_1_37fd804e"),
            fieldAs must contain("xyz"),
            recoveredAs must contain(Some(123L)),
            df.count() must beEqualTo(8L)
          ).reduce(_ and _)
        }
      }
    }
  }

  def e4 = Files[IO].tempDirectory.use { tmpDir =>
    val adBreakEndEvent = SnowplowEvent.UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow.media", "ad_break_end_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj()
        )
      )
    )

    val resources = for {
      inputs <- Resource.eval(EventUtils.inputEvents(2, EventUtils.good(ue = adBreakEndEvent)))
      tokened <- Resource.eval(inputs.traverse(_.tokened))
      env <- TestSparkEnvironment.build(target, tmpDir, List(tokened))
    } yield env

    val io = resources.use { env =>
      Processing
        .stream(env)
        .compile
        .drain
    }

    io *> {
      sparkForAssertions(sparkConfig(tmpDir)).use { spark =>
        IO.blocking(readTable(spark, tmpDir)).map { df =>
          val cols = df.columns.toSeq

          List[MatchResult[Any]](
            cols must not contain (beMatching("unstruct_event_.*".r)),
            df.count() must beEqualTo(4L)
          ).reduce(_ and _)
        }
      }
    }
  }

  def e5 = Files[IO].tempDirectory.use { tmpDir =>
    val ue = SnowplowEvent.UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "no-fields", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj(
            "a" -> Json.fromString("xyz"),
            "b" -> Json.fromString("abc")
          )
        )
      )
    )

    val resources = for {
      inputs <- Resource.eval(EventUtils.inputEvents(2, EventUtils.good(ue = ue)))
      tokened <- Resource.eval(inputs.traverse(_.tokened))
      env <- TestSparkEnvironment.build(target, tmpDir, List(tokened))
    } yield env

    val io = resources.use { env =>
      Processing
        .stream(env)
        .compile
        .drain
    }

    io *> {
      sparkForAssertions(sparkConfig(tmpDir)).use { spark =>
        IO.blocking(readTable(spark, tmpDir)).map { df =>
          import spark.implicits._
          val cols = df.columns.toSeq
          val ues  = df.select("unstruct_event_myvendor_no_fields_1").as[String].collect().toSeq

          List[MatchResult[Any]](
            cols must contain("unstruct_event_myvendor_no_fields_1"),
            ues must contain("""{"a":"xyz","b":"abc"}"""),
            df.count() must beEqualTo(4L)
          ).reduce(_ and _)
        }
      }
    }
  }

  def e6 = Files[IO].tempDirectory.use { tmpDir =>
    val adBreakEndEvent = SnowplowEvent.Contexts(
      List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow.media", "ad_break_end_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj()
        )
      )
    )

    val resources = for {
      inputs <- Resource.eval(EventUtils.inputEvents(2, EventUtils.good(contexts = adBreakEndEvent)))
      tokened <- Resource.eval(inputs.traverse(_.tokened))
      env <- TestSparkEnvironment.build(target, tmpDir, List(tokened))
    } yield env

    val io = resources.use { env =>
      Processing
        .stream(env)
        .compile
        .drain
    }

    io *> {
      sparkForAssertions(sparkConfig(tmpDir)).use { spark =>
        IO.blocking(readTable(spark, tmpDir)).map { df =>
          val cols = df.columns.toSeq

          List[MatchResult[Any]](
            cols must not contain (beMatching("contexts_.*".r)),
            df.count() must beEqualTo(4L)
          ).reduce(_ and _)
        }
      }
    }
  }

  def e7 = Files[IO].tempDirectory.use { tmpDir =>
    val contexts = SnowplowEvent.Contexts(
      List(
        SelfDescribingData(
          SchemaKey("myvendor", "no-fields", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Json.obj(
            "a" -> Json.fromString("xyz"),
            "b" -> Json.fromString("abc")
          )
        )
      )
    )

    val resources = for {
      inputs <- Resource.eval(EventUtils.inputEvents(2, EventUtils.good(contexts = contexts)))
      tokened <- Resource.eval(inputs.traverse(_.tokened))
      env <- TestSparkEnvironment.build(target, tmpDir, List(tokened))
    } yield env

    val io = resources.use { env =>
      Processing
        .stream(env)
        .compile
        .drain
    }

    io *> {
      sparkForAssertions(sparkConfig(tmpDir)).use { spark =>
        IO.blocking(readTable(spark, tmpDir)).map { df =>
          import spark.implicits._
          val cols = df.columns.toSeq
          val vs   = df.select("contexts_myvendor_no_fields_1").as[Option[List[String]]].collect().toSeq

          List[MatchResult[Any]](
            cols must contain("contexts_myvendor_no_fields_1"),
            vs must contain(Some(List("""{"a":"xyz","b":"abc","_schema_version":"1-0-0"}"""))),
            df.count() must beEqualTo(4L)
          ).reduce(_ and _)
        }
      }
    }
  }

}

object AbstractSparkSpec {

  /** A spark session just used for making assertions, not for running the code under test */
  private def sparkForAssertions(config: Map[String, String]): Resource[IO, SparkSession] = {
    val io = IO.blocking {
      SparkSession
        .builder()
        .appName("testing")
        .master(s"local[*]")
        .config(config)
        .getOrCreate()
    }
    Resource.make(io)(s => IO.blocking(s.close()))
  }

}
