/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.implicits._
import eu.timepit.refined.types.all.PosInt
import io.circe._
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import io.circe.generic.extras.Configuration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain

import java.net.URI
import java.time.Instant

case class KinesisSourceConfig(
  appName: String,
  streamName: String,
  initialPosition: KinesisSourceConfig.InitPosition,
  retrievalMode: KinesisSourceConfig.Retrieval,
  bufferSize: PosInt,
  customEndpoint: Option[URI],
  dynamodbCustomEndpoint: Option[URI],
  cloudwatchCustomEndpoint: Option[URI]
)

object KinesisSourceConfig {

  implicit val config: Configuration = Configuration.default.withDefaults

  implicit val posIntDecoder: Decoder[PosInt] =
    Decoder.decodeInt.emap { int =>
      if (int > 0) PosInt.from(int)
      else s"Positive integer expected but [$int] is provided".asLeft[PosInt]
    }

  def getRuntimeRegion: Either[Throwable, Region] =
    Either.catchNonFatal((new DefaultAwsRegionProviderChain).getRegion)

  sealed trait InitPosition

  object InitPosition {
    case object Latest extends InitPosition

    case object TrimHorizon extends InitPosition

    case class AtTimestamp(timestamp: Instant) extends InitPosition

    implicit val decoder: Decoder[InitPosition] = deriveConfiguredDecoder[InitPosition]
  }

  sealed trait Retrieval

  object Retrieval {
    case class Polling(maxRecords: Int) extends Retrieval

    case object FanOut extends Retrieval

    implicit val decoder: Decoder[Retrieval] = deriveConfiguredDecoder[Retrieval]
  }

  implicit val decoder: Decoder[KinesisSourceConfig] = deriveConfiguredDecoder[KinesisSourceConfig]
}
