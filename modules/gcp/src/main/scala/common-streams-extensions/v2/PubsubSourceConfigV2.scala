/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import cats.Show
import io.circe.Decoder
import io.circe.generic.semiauto._
import io.circe.config.syntax._
import com.google.pubsub.v1.ProjectSubscriptionName

import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.snowplow.pubsub.GcpUserAgent

case class PubsubSourceConfigV2(
  subscription: PubsubSourceConfigV2.Subscription,
  parallelPullFactor: BigDecimal,
  durationPerAckExtension: FiniteDuration,
  minRemainingDeadline: Double,
  gcpUserAgent: GcpUserAgent,
  maxPullsPerTransportChannel: Int,
  progressTimeout: FiniteDuration,
  prefetchMin: Int,
  prefetchMax: Int
)

object PubsubSourceConfigV2 {

  case class Subscription(projectId: String, subscriptionId: String)

  object Subscription {
    implicit def show: Show[Subscription] = Show[Subscription] { s =>
      ProjectSubscriptionName.of(s.projectId, s.subscriptionId).toString
    }
  }

  private implicit def subscriptionDecoder: Decoder[Subscription] =
    Decoder.decodeString
      .map(_.split("/"))
      .emap {
        case Array("projects", projectId, "subscriptions", subscriptionId) =>
          Right(Subscription(projectId, subscriptionId))
        case _ =>
          Left("Expected format: projects/<project>/subscriptions/<subscription>")
      }

  implicit def decoder: Decoder[PubsubSourceConfigV2] = deriveDecoder[PubsubSourceConfigV2]
}
