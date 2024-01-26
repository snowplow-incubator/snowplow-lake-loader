/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.sources

import io.circe.Decoder
import io.circe.generic.semiauto._
import io.circe.config.syntax._

import scala.concurrent.duration.FiniteDuration

case class PubsubSourceConfig(
  subscription: PubsubSourceConfig.Subscription,
  parallelPullCount: Int,
  bufferMaxBytes: Long,
  maxAckExtensionPeriod: FiniteDuration,
  minDurationPerAckExtension: FiniteDuration,
  maxDurationPerAckExtension: FiniteDuration
)

object PubsubSourceConfig {

  case class Subscription(projectId: String, subscriptionId: String)

  private implicit def subscriptionDecoder: Decoder[Subscription] =
    Decoder.decodeString
      .map(_.split("/"))
      .emap {
        case Array("projects", projectId, "subscriptions", subscriptionId) =>
          Right(Subscription(projectId, subscriptionId))
        case _ =>
          Left("Expected format: projects/<project>/subscriptions/<subscription>")
      }

  implicit def decoder: Decoder[PubsubSourceConfig] = deriveDecoder[PubsubSourceConfig]
}
