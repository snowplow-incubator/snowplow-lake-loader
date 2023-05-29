package com.snowplowanalytics.snowplow.sources

import fs2.Stream

trait SourceAndAck[F[_]] {
  def stream(config: SourceConfig, processor: EventProcessor[F]): Stream[F, Nothing]
}
