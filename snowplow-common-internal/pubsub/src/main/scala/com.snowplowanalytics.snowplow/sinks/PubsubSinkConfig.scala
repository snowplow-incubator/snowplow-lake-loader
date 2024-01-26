/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.sinks

import io.circe.Decoder
import io.circe.generic.semiauto._

case class PubsubSinkConfig(
  topic: PubsubSinkConfig.Topic,
  batchSize: Long,
  requestByteThreshold: Long
)

object PubsubSinkConfig {
  case class Topic(projectId: String, topicId: String)

  private implicit def topicDecoder: Decoder[Topic] =
    Decoder.decodeString
      .map(_.split("/"))
      .emap {
        case Array("projects", projectId, "topics", topicId) =>
          Right(Topic(projectId, topicId))
        case _ =>
          Left("Expected format: projects/<project>/topics/<topic>")
      }

  implicit def decoder: Decoder[PubsubSinkConfig] = deriveDecoder[PubsubSinkConfig]
}
