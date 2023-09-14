/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import io.circe._
import io.circe.generic.semiauto._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain

import scala.util.Try

import java.net.URI
import java.time.Instant

case class KinesisSourceConfig(
  appName: String,
  streamName: String,
  region: Option[Region],
  initialPosition: KinesisSourceConfig.InitPosition,
  retrievalMode: KinesisSourceConfig.Retrieval,
  bufferSize: Int,
  customEndpoint: Option[URI],
  dynamodbCustomEndpoint: Option[URI],
  cloudwatchCustomEndpoint: Option[URI]
)

object KinesisSourceConfig {

  def getRuntimeRegion: Either[Throwable, Region] =
    Try((new DefaultAwsRegionProviderChain).getRegion).toEither

  implicit val regionDecoder: Decoder[Region] =
    Decoder
      .decodeString
      .emap { s => Try(Region.of(s)).toEither.left.map(_.toString) }

  sealed trait InitPosition

  object InitPosition {
    case object Latest extends InitPosition

    case object TrimHorizon extends InitPosition

    case class AtTimestamp(timestamp: Instant) extends InitPosition

    implicit val decoder: Decoder[InitPosition] = deriveDecoder[InitPosition]
  }

  sealed trait Retrieval

  object Retrieval {
    case class Polling(maxRecords: Int) extends Retrieval

    case object FanOut extends Retrieval

    implicit val decoder: Decoder[Retrieval] = deriveDecoder[Retrieval]
  }

  implicit val decoder: Decoder[KinesisSourceConfig] = deriveDecoder[KinesisSourceConfig]
}
