package com.snowplowanalytics.snowplow.sources.internal

case class LowLevelEvents[C](events: List[Array[Byte]], ack: C)
