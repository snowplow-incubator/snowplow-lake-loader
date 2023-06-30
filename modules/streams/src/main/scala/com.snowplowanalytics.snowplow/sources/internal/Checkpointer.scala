/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.internal

import cats.Monoid

/**
 * Checkpoints messages to the input stream so that we don't read them again. A single checkpointer
 * might be responsible for multiple incoming messages.
 * @tparam F
 *   An effect
 * @tparam C
 *   Source-specific type describing the outstading messages
 */
trait Checkpointer[F[_], C] extends Monoid[C] {

  /** Trigger checkpointing. Must be called only when all message processing is complete */
  def checkpoint(c: C): F[Unit]

}

object Checkpointer {

  def apply[F[_], C: Monoid](f: C => F[Unit]): Checkpointer[F, C] =
    new Checkpointer[F, C] {
      override def checkpoint(c: C): F[Unit] = f(c)
      override def empty: C = Monoid[C].empty
      override def combine(x: C, y: C): C = Monoid[C].combine(x, y)
    }

}
