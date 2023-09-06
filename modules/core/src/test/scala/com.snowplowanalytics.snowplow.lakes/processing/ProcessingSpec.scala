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

import java.nio.charset.StandardCharsets
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.lakes.MockEnvironment
import com.snowplowanalytics.snowplow.lakes.MockEnvironment.Action
import com.snowplowanalytics.snowplow.sources.TokenedEvents

import java.nio.ByteBuffer

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = s2"""
  The lake loader should:
    Ack received events at the end of a single window $e1
    Send badly formatted events to the bad sink $e2
    Write multiple windows of events in order $e3
    Write multiple batches in a single window when batch exceeds cutoff $e4
    Write good batches and bad events when a window contains both $e5
  """

  def e1 =
    for {
      inputs <- generateEvents.take(2).compile.toList
      control <- MockEnvironment.build(List(inputs))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-0", 4)),
        Action.CommittedToTheLake(List("view-0")),
        Action.Checkpointed(inputs.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-0"))
      )
    )

  def e2 =
    for {
      inputs <- generateBadlyFormatted.take(3).compile.toList
      control <- MockEnvironment.build(List(inputs))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.SentToBad(2),
        Action.SentToBad(2),
        Action.SentToBad(2),
        Action.Checkpointed(inputs.map(_.ack))
      )
    )

  def e3 =
    for {
      window1 <- generateEvents.take(2).compile.toList
      window2 <- generateEvents.take(7).compile.toList
      window3 <- generateEvents.take(5).compile.toList
      control <- MockEnvironment.build(List(window1, window2, window3))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-0", 4)),
        Action.CommittedToTheLake(List("view-0")),
        Action.Checkpointed(window1.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-0")),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-1", 14)),
        Action.CommittedToTheLake(List("view-1")),
        Action.Checkpointed(window2.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-1")),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-2", 10)),
        Action.CommittedToTheLake(List("view-2")),
        Action.Checkpointed(window3.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-2"))
      )
    )

  def e4 =
    for {
      inputs <- generateEvents.take(3).compile.toList
      control <- MockEnvironment.build(List(inputs))
      environment = control.environment.copy(inMemBatchBytes = 1L) // Drop the allowed max bytes
      _ <- Processing.stream(environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-0", 2)),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-1", 2)),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-2", 2)),
        Action.CommittedToTheLake(List("view-2", "view-1", "view-0")),
        Action.Checkpointed(inputs.map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-2", "view-1", "view-0"))
      )
    )

  def e5 =
    for {
      bads1 <- generateBadlyFormatted.take(3).compile.toList
      goods1 <- generateEvents.take(3).compile.toList
      bads2 <- generateBadlyFormatted.take(3).compile.toList
      goods2 <- generateEvents.take(3).compile.toList
      control <- MockEnvironment.build(List(bads1 ::: goods1 ::: bads2 ::: goods2))
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.CreatedTable,
        Action.SentToBad(2),
        Action.SentToBad(2),
        Action.SentToBad(2),
        Action.SentToBad(2),
        Action.SentToBad(2),
        Action.SentToBad(2),
        Action.SavedDataFrameToDisk(DataFrameOnDisk("view-0", 12)),
        Action.CommittedToTheLake(List("view-0")),
        Action.Checkpointed((bads1 ::: goods1 ::: bads2 ::: goods2).map(_.ack)),
        Action.RemovedDataFramesFromDisk(List("view-0"))
      )
    )
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
          ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))
        }
        TokenedEvents(serialized, ack)
      }
    }.repeat

  def generateBadlyFormatted: Stream[IO, TokenedEvents] =
    Stream.eval {
      IO.unique.map { token =>
        val serialized = List("nonsense1", "nonsense2").map { e =>
          ByteBuffer.wrap(e.getBytes(StandardCharsets.UTF_8))
        }
        TokenedEvents(serialized, token)
      }
    }.repeat

}
