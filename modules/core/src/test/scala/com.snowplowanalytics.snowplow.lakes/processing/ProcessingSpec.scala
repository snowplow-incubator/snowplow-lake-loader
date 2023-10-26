/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.processing

import cats.effect.IO
import fs2.Stream
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
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-0", 4)),
        Action.CommittedToTheLake(List("view-0")),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(inputs.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-0"))
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
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.SentToBad(2),
        Action.Checkpointed(inputs.map(_.ack))
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
        Action.AddedReceivedCountMetric(2),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-0", 2)),
        Action.CommittedToTheLake(List("view-0")),
        Action.AddedCommittedCountMetric(2),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(window1.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-0")),

        /* window 2 */
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-1", 6)),
        Action.CommittedToTheLake(List("view-1")),
        Action.AddedCommittedCountMetric(6),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(window2.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-1")),

        /* window 3 */
        Action.AddedReceivedCountMetric(2),
        Action.AddedReceivedCountMetric(2),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-2", 4)),
        Action.CommittedToTheLake(List("view-2")),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(window3.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-2"))
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
        Action.AddedReceivedCountMetric(2),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-0", 2)),
        Action.AddedReceivedCountMetric(2),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-1", 2)),
        Action.AddedReceivedCountMetric(2),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-2", 2)),
        Action.CommittedToTheLake(List("view-2", "view-1", "view-0")),
        Action.AddedCommittedCountMetric(6),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(inputs.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-2", "view-1", "view-0"))
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
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-0", 8)),
        Action.CommittedToTheLake(List("view-0")),
        Action.AddedCommittedCountMetric(8),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed((bads1 ::: goods1 ::: bads2 ::: goods2).map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-0"))
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
        Action.SetLatencyMetric(42123.millis),
        Action.AddedReceivedCountMetric(2),
        Action.SetLatencyMetric(42123.millis),
        Action.AddedReceivedCountMetric(2),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-0", 4)),
        Action.CommittedToTheLake(List("view-0")),
        Action.AddedCommittedCountMetric(4),
        Action.SetProcessingLatencyMetric(MockEnvironment.WindowDuration),
        Action.Checkpointed(inputs.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-0"))
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
        val serialized = List(event1, event2).map { e =>
          StandardCharsets.UTF_8.encode(e.toTsv)
        }
        TokenedEvents(serialized, ack, None)
      }
    }.repeat

  def generateBadlyFormatted: Stream[IO, TokenedEvents] =
    Stream.eval {
      IO.unique.map { token =>
        val serialized = List("nonsense1", "nonsense2").map(StandardCharsets.UTF_8.encode(_))
        TokenedEvents(serialized, token, None)
      }
    }.repeat

}
