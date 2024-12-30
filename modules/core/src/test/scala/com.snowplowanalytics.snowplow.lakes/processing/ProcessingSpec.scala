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

import cats.implicits._
import cats.effect.IO
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import io.circe.Json
import cats.effect.testkit.TestControl

import java.time.Instant
import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent
import com.snowplowanalytics.snowplow.lakes.{MockEnvironment, RuntimeService}
import com.snowplowanalytics.snowplow.lakes.MockEnvironment.Action

class ProcessingSpec extends Specification with CatsEffect {

  def is = s2"""
  The lake loader should:
    Ack received events at the end of a single window $e1
    Send badly formatted events to the bad sink $e2
    Write multiple windows of events in order $e3
    Write multiple batches in a single window when batch exceeds cutoff $e4
    Write good batches and bad events when a window contains both $e5
    Set the latency metric based off the message timestamp $e6
    Load events with a known schema $e7
    Send failed events for an unrecognized schema $e8
    Crash and exit for an unrecognized schema, if exitOnMissingIgluSchema is true $e9
  """

  def e1 = {
    val io = for {
      inputs <- EventUtils.inputEvents(2, EventUtils.good())
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.InitializedLocalDataFrame("v19700101000000_0", 4),
        Action.CommittedToTheLake(List("v19700101000000_0")),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.Checkpointed(tokened.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000_0")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e2 = {
    val io = for {
      tokened <- List.fill(3)(EventUtils.badlyFormatted).sequence
      control <- MockEnvironment.build(List(tokened))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.Checkpointed(tokened.map(_.ack))
      )
    )

    TestControl.executeEmbed(io)
  }

  def e3 = {
    val io = for {
      inputs1 <- EventUtils.inputEvents(1, EventUtils.good())
      window1 <- inputs1.traverse(_.tokened)
      inputs2 <- EventUtils.inputEvents(3, EventUtils.good())
      window2 <- inputs2.traverse(_.tokened)
      inputs3 <- EventUtils.inputEvents(2, EventUtils.good())
      window3 <- inputs3.traverse(_.tokened)
      control <- MockEnvironment.build(List(window1, window2, window3))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,

        /* window 1 */
        Action.AddedReceivedCountMetric(2),
        Action.InitializedLocalDataFrame("v19700101000000_0", 2),
        Action.CommittedToTheLake(List("v19700101000000_0")),
        Action.AddedCommittedCountMetric(2),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.Checkpointed(window1.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000_0"),

        /* window 2 */
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.InitializedLocalDataFrame("v19700101000052_0", 6),
        Action.CommittedToTheLake(List("v19700101000052_0")),
        Action.AddedCommittedCountMetric(6),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(window2.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000052_0"),

        /* window 3 */
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.InitializedLocalDataFrame("v19700101000134_0", 4),
        Action.CommittedToTheLake(List("v19700101000134_0")),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(window3.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000134_0")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e4 = {
    val io = for {
      inputs <- EventUtils.inputEvents(3, EventUtils.good())
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      environment = control.environment.copy(inMemBatchBytes = 1L) // Drop the allowed max bytes
      _ <- Processing.stream(environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.AddedReceivedCountMetric(2),
        Action.InitializedLocalDataFrame("v19700101000000_0", 2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000_0", 2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000_0", 2),
        Action.CommittedToTheLake(List("v19700101000000_0")),
        Action.AddedCommittedCountMetric(6),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.Checkpointed(tokened.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000_0")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e5 = {
    val io = for {
      bads1 <- List.fill(3)(EventUtils.badlyFormatted).sequence
      goods1 <- EventUtils.inputEvents(3, EventUtils.good()).flatMap(_.traverse(_.tokened))
      bads2 <- List.fill(1)(EventUtils.badlyFormatted).sequence
      goods2 <- EventUtils.inputEvents(1, EventUtils.good()).flatMap(_.traverse(_.tokened))
      control <- MockEnvironment.build(List(bads1 ::: goods1 ::: bads2 ::: goods2))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.InitializedLocalDataFrame("v19700101000000_0", 8),
        Action.CommittedToTheLake(List("v19700101000000_0")),
        Action.AddedCommittedCountMetric(8),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.Checkpointed((bads1 ::: goods1 ::: bads2 ::: goods2).map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000_0")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e6 = {
    val messageTime = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime = Instant.parse("2023-10-24T10:00:42.123Z").minusMillis(MockEnvironment.TimeTakenToCreateTable.toMillis)

    val io = for {
      inputs <- EventUtils.inputEvents(2, EventUtils.good())
      tokened <- inputs.traverse(_.tokened).map {
                   _.map {
                     _.copy(earliestSourceTstamp = Some(messageTime))
                   }
                 }
      control <- MockEnvironment.build(List(tokened))
      _ <- IO.sleep(processTime.toEpochMilli.millis)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.SetLatencyMetric(42123.millis),
        Action.AddedReceivedCountMetric(2),
        Action.SetLatencyMetric(42123.millis),
        Action.AddedReceivedCountMetric(2),
        Action.InitializedLocalDataFrame("v20231024100032_0", 4),
        Action.CommittedToTheLake(List("v20231024100032_0")),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.Checkpointed(tokened.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v20231024100032_0")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e7 = {

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

    val io = for {
      inputs <- EventUtils.inputEvents(1, EventUtils.good(ue = ueGood700))
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.AddedReceivedCountMetric(2),
        Action.InitializedLocalDataFrame("v19700101000000_0", 2),
        Action.CommittedToTheLake(List("v19700101000000_0")),
        Action.AddedCommittedCountMetric(2),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.Checkpointed(tokened.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000_0")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e8 = {

    val ueDoesNotExist = SnowplowEvent.UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "doesnotexit", "jsonschema", SchemaVer.Full(7, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val io = for {
      inputs <- EventUtils.inputEvents(1, EventUtils.good(ue = ueDoesNotExist))
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(1),
        Action.SentToBad(1),
        Action.InitializedLocalDataFrame("v19700101000000_0", 1),
        Action.CommittedToTheLake(List("v19700101000000_0")),
        Action.AddedCommittedCountMetric(1),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration + MockEnvironment.TimeTakenToCreateTable),
        Action.Checkpointed(tokened.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000_0")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e9 = {

    val ueDoesNotExist = SnowplowEvent.UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey("myvendor", "doesnotexit", "jsonschema", SchemaVer.Full(7, 0, 0)),
          Json.obj(
            "col_a" -> Json.fromString("xyz")
          )
        )
      )
    )

    val io = for {
      inputs <- EventUtils.inputEvents(1, EventUtils.good(ue = ueDoesNotExist))
      tokened <- inputs.traverse(_.tokened)
      control <- MockEnvironment.build(List(tokened))
      environment = control.environment.copy(exitOnMissingIgluSchema = true)
      _ <- Processing.stream(environment).compile.drain.voidError
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SubscribedToStream,
        Action.CreatedTable,
        Action.AddedReceivedCountMetric(2),
        Action.BecameUnhealthy(RuntimeService.Iglu)
      )
    )

    TestControl.executeEmbed(io)
  }

}
