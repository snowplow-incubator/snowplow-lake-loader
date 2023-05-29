package com.snowplowanalytics.snowplow

import fs2.Pipe
import cats.effect.kernel.Unique

package object sources {
  type EventProcessor[F[_]] = Pipe[F, TokenedEvents, Unique.Token]
}
