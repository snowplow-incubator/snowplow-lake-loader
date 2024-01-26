/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.sources.internal

import cats.{Applicative, Monoid}

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
  def ack(c: C): F[Unit]

  /** Nack un-processed messages.  Used during shutdown */
  def nack(c: C): F[Unit]

}

object Checkpointer {

  /** A simple implementation that does ack only and no nacks */
  def acksOnly[F[_]: Applicative, C: Monoid](f: C => F[Unit]): Checkpointer[F, C] =
    new Checkpointer[F, C] {
      override def ack(c: C): F[Unit] = f(c)
      override def nack(c: C): F[Unit] = Applicative[F].unit
      override def empty: C = Monoid[C].empty
      override def combine(x: C, y: C): C = Monoid[C].combine(x, y)
    }

}
