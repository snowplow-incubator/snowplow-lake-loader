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

import cats.effect.kernel.Unique

/**
 * Local in-memory state which is accumulated as a window gets processed
 *
 * The data cached in this state is needed when finalizing the window, i.e. flushing events from
 * disk to storage
 *
 * @param tokens
 *   Tokens given to us by the sources library. Emitting these tokens at the end of the window will
 *   trigger checkpointing/acking of the source events.
 * @param framesOnDisk
 *   Spark DataFrames which we cached on disk while processing this window
 * @param nonAtomicColumnNames
 *   Names of the columns which will be written out by the loader
 */
private[processing] case class WindowState(
  tokens: List[Unique.Token],
  framesOnDisk: List[DataFrameOnDisk],
  nonAtomicColumnNames: Set[String]
)

private[processing] object WindowState {
  def empty: WindowState = WindowState(Nil, Nil, Set.empty)
}
