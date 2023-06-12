package com.snowplowanalytics.snowplow.sinks

import io.circe.Decoder
import io.circe.generic.semiauto._

case class KafkaSinkConfig(
  topicName: String,
  bootstrapServers: String,
  producerConf: Map[String, String]
)

object KafkaSinkConfig {
  implicit def decoder: Decoder[KafkaSinkConfig] = deriveDecoder[KafkaSinkConfig]
}
