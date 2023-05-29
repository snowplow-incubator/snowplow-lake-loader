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
