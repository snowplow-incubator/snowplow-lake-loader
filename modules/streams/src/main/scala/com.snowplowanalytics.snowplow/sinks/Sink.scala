package com.snowplowanalytics.snowplow.sinks

/**
 * A common interface over the external sinks that Snowplow can write to
 *
 * Implementations of this trait are provided by the sinks library (e.g. kinesis, kafka, pubsub)
 */
trait Sink[F[_]] {

  /**
   * Writes a batch of events to the external sink, handling partition keys and message attributes
   */
  def sink(batch: List[Sinkable]): F[Unit]

  /** Writes a batch of events to the sink using an empty partition key and attributes */
  def sinkSimple(batch: List[Array[Byte]]): F[Unit] =
    sink(batch.map(Sinkable(_, None, Map.empty)))

}

object Sink {

  def apply[F[_]](f: List[Sinkable] => F[Unit]): Sink[F] = new Sink[F] {
    def sink(batch: List[Sinkable]): F[Unit] = f(batch)
  }
}
