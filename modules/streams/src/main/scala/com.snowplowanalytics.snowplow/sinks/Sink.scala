package com.snowplowanalytics.snowplow.sinks

trait Sink[F[_]] {

  def sink(batch: List[Sinkable]): F[Unit]

  def sinkSimple(batch: List[Array[Byte]]): F[Unit] =
    sink(batch.map(Sinkable(_, None, Map.empty)))

}

object Sink {

  def apply[F[_]](f: List[Sinkable] => F[Unit]): Sink[F] = new Sink[F] {
    def sink(batch: List[Sinkable]): F[Unit] = f(batch)
  }
}
