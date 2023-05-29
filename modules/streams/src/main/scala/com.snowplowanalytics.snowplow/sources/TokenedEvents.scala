package com.snowplowanalytics.snowplow.sources

import cats.effect.kernel.Unique

case class TokenedEvents(events: List[Array[Byte]], ack: Unique.Token)
