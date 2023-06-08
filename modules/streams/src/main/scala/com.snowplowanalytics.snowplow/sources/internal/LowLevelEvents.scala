package com.snowplowanalytics.snowplow.sources.internal

/**
 * The events and checkpointable item emitted by a LowLevelSource
 *
 * This library uses LowLevelEvents internally, but it is never exposed to the high level event
 * processor
 */
case class LowLevelEvents[C](events: List[Array[Byte]], ack: C)
