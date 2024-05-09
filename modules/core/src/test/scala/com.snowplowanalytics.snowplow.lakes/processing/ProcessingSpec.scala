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
import fs2.{Chunk, Stream}
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.lakes.MockEnvironment
import com.snowplowanalytics.snowplow.lakes.MockEnvironment.Action
import com.snowplowanalytics.snowplow.sources.TokenedEvents

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = s2"""
  The lake loader should:
    Ack received events at the end of a single window $e1
    Send badly formatted events to the bad sink $e2
    Write multiple windows of events in order $e3
    Write multiple batches in a single window when batch exceeds cutoff $e4
    Write good batches and bad events when a window contains both $e5
    Set the latency metric based off the message timestamp $e6
  """

  def e1 = {
    val io = for {
      inputs <- generateEvents.take(2).compile.toList
      control <- MockEnvironment.build(List(inputs))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 4),
        Action.CommittedToTheLake("v19700101000000"),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(inputs.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e2 = {
    val io = for {
      inputs <- generateBadlyFormatted.take(3).compile.toList
      control <- MockEnvironment.build(List(inputs))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.Checkpointed(inputs.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e3 = {
    val io = for {
      window1 <- generateEvents.take(1).compile.toList
      window2 <- generateEvents.take(3).compile.toList
      window3 <- generateEvents.take(2).compile.toList
      control <- MockEnvironment.build(List(window1, window2, window3))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,

        /* window 1 */
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 2),
        Action.CommittedToTheLake("v19700101000000"),
        Action.AddedCommittedCountMetric(2),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(window1.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000"),

        /* window 2 */
        Action.InitializedLocalDataFrame("v19700101000042"),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000042", 6),
        Action.CommittedToTheLake("v19700101000042"),
        Action.AddedCommittedCountMetric(6),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(window2.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000042"),

        /* window 3 */
        Action.InitializedLocalDataFrame("v19700101000124"),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000124", 4),
        Action.CommittedToTheLake("v19700101000124"),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(window3.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000124")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e4 = {
    val io = for {
      inputs <- generateEvents.take(3).compile.toList
      control <- MockEnvironment.build(List(inputs))
      environment = control.environment.copy(inMemBatchBytes = 1L) // Drop the allowed max bytes
      _ <- Processing.stream(environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 2),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v19700101000000", 2),
        Action.CommittedToTheLake("v19700101000000"),
        Action.AddedCommittedCountMetric(6),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(inputs.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e5 = {
    val io = for {
      bads1 <- generateBadlyFormatted.take(3).compile.toList
      goods1 <- generateEvents.take(3).compile.toList
      bads2 <- generateBadlyFormatted.take(1).compile.toList
      goods2 <- generateEvents.take(1).compile.toList
      control <- MockEnvironment.build(List(bads1 ::: goods1 ::: bads2 ::: goods2))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v19700101000000"),
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
        Action.AppendedRowsToDataFrame("v19700101000000", 8),
        Action.CommittedToTheLake("v19700101000000"),
        Action.AddedCommittedCountMetric(8),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed((bads1 ::: goods1 ::: bads2 ::: goods2).map(_.ack)),
        Action.RemovedDataFrameFromDisk("v19700101000000")
      )
    )

    TestControl.executeEmbed(io)
  }

  def e6 = {
    val messageTime = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime = Instant.parse("2023-10-24T10:00:42.123Z")

    val io = for {
      inputs <- generateEvents.take(2).compile.toList.map {
                  _.map {
                    _.copy(earliestSourceTstamp = Some(messageTime))
                  }
                }
      control <- MockEnvironment.build(List(inputs))
      _ <- IO.sleep(processTime.toEpochMilli.millis)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.InitializedLocalDataFrame("v20231024100042"),
        Action.SetLatencyMetric(42123.millis),
        Action.AddedReceivedCountMetric(2),
        Action.SetLatencyMetric(42123.millis),
        Action.AddedReceivedCountMetric(2),
        Action.AppendedRowsToDataFrame("v20231024100042", 4),
        Action.CommittedToTheLake("v20231024100042"),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(inputs.map(_.ack)),
        Action.RemovedDataFrameFromDisk("v20231024100042")
      )
    )

    TestControl.executeEmbed(io)
  }
}

object ProcessingSpec {

  def generateEvents: Stream[IO, TokenedEvents] =
    Stream.eval {
      for {
        ack <- IO.unique
        eventId1 <- IO.randomUUID
        eventId2 <- IO.randomUUID
        collectorTstamp <- IO.realTimeInstant
      } yield {
        val event1 = Event.minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0")
        val event2 = Event.minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
        val serialized = Chunk(event1, event2).map { e =>
          StandardCharsets.UTF_8.encode(e.toTsv)
        }
        TokenedEvents(serialized, ack, None)
      }
    }.repeat

  def generateBadlyFormatted: Stream[IO, TokenedEvents] =
    Stream.eval {
      IO.unique.map { token =>
        val serialized = Chunk("nonsense1", "nonsense2").map(StandardCharsets.UTF_8.encode(_))
        TokenedEvents(serialized, token, None)
      }
    }.repeat

}
