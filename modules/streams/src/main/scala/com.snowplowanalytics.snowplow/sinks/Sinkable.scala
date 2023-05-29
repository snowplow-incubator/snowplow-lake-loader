package com.snowplowanalytics.snowplow.sinks

case class Sinkable(
  bytes: Array[Byte],
  partitionKey: Option[String],
  attributes: Map[String, String]
)
