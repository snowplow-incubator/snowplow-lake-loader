package com.snowplowanalytics.snowplow.sources

import fs2.Stream

/**
 * The machinery for sourcing events from and external stream and then acking/checkpointing them.
 *
 * Implementations of this trait are provided by the sources library (e.g. kinesis, kafka, pubsub)
 * whereas implementations of [[EventProcessor]] are provided by the specific application (e.g.
 * enrich, transformer, loaders)
 */
trait SourceAndAck[F[_]] {

  /**
   * Wraps the [[EventProcessor]] to create a Stream which, when compiled drained, causes events to
   * flow through the processor.
   *
   * @param config
   *   Configures how events are fed into the processor, e.g. whether to use timed windows
   * @param processor
   *   The EventProcessor, which is implemented by the specific application, e.g. enrich or a loader
   * @return
   *   A stream which should be compiled and drained
   */
  def stream(config: EventProcessingConfig, processor: EventProcessor[F]): Stream[F, Nothing]
}
