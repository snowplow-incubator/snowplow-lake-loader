/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
