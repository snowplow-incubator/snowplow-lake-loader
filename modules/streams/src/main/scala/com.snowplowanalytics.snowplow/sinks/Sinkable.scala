package com.snowplowanalytics.snowplow.sinks

/**
 * A single event that can be written to the external sink
 *
 * @param bytes
 *   the serialized content of this event
 * @param partitionKey
 *   optionally controls which partition the event is written to
 * @param attributes
 *   optionally add attributes/headers to the event, if the sink supports this feature
 */
case class Sinkable(
  bytes: Array[Byte],
  partitionKey: Option[String],
  attributes: Map[String, String]
)
