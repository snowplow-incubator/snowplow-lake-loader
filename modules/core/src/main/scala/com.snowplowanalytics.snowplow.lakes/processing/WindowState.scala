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

import cats.effect.kernel.Unique
import cats.effect.{Ref, Sync}
import cats.implicits._

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

/**
 * Local in-memory state which is accumulated as a window gets processed
 *
 * The data cached in this state is needed when finalizing the window, i.e. flushing events from
 * disk to storage
 *
 * @param tokens
 *   Tokens given to us by the sources library. Emitting these tokens at the end of the window will
 *   trigger checkpointing/acking of the source events.
 * @param startTime
 *   The time this window was initially opened
 * @param nonAtomicColumnNames
 *   Names of the columns which will be written out by the loader
 * @param numEvents
 *   The number of events in this window
 * @param viewNames
 *   The names by which current DataFrames are known to the Spark catalog, keyed by output partition
 *   (e.g. by event name)
 */
private[processing] case class WindowState(
  tokens: List[Unique.Token],
  startTime: Instant,
  nonAtomicColumnNames: Set[String],
  numEvents: Int,
  viewNames: Map[Option[String], String]
) {

  /** The base name for all view names */
  val baseName: String =
    WindowState.formatter.format(startTime)

}

private[processing] object WindowState {
  private val formatter: DateTimeFormatter =
    DateTimeFormatter
      .ofPattern("'v'yyyyMMddHHmmss")
      .withZone(ZoneOffset.UTC)

  def build[F[_]: Sync]: F[WindowState] =
    Sync[F].realTimeInstant.map { now =>
      WindowState(Nil, now, Set.empty, 0, Map.empty)
    }

  def generateAndSaveViewName[F[_]](ref: Ref[F, WindowState], partitionName: Option[String]): F[String] =
    ref.modify { state =>
      val viewName  = s"${state.baseName}_${state.viewNames.size}"
      val viewNames = state.viewNames + (partitionName -> viewName)
      (state.copy(viewNames = viewNames), viewName)
    }
}
