package com.snowplowanalytics.snowplow.sources

import io.circe.Decoder
import io.circe.generic.semiauto._

case class KafkaSourceConfig(
  topicName: String,
  bootstrapServers: String,
  consumerConf: Map[String, String]
)

object KafkaSourceConfig {
  implicit def decoder: Decoder[KafkaSourceConfig] = deriveDecoder[KafkaSourceConfig]
}
