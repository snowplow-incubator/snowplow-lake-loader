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
import software.amazon.kinesis.common.{InitialPositionInStream, InitialPositionInStreamExtended}

import java.net.URI
import java.time.Instant
import java.util.Date

case class KinesisSourceConfig(
  appName: String,
  streamName: String,
  initialPosition: InitialPositionInStreamExtended,
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

  case class InitPositionRaw(`type`: String, timestamp: Option[Instant])
  implicit val initPositionRawDecoder: Decoder[InitPositionRaw] = deriveConfiguredDecoder[InitPositionRaw]

  implicit val initPositionDecoder: Decoder[InitialPositionInStreamExtended] =
    Decoder.instance { cur =>
      for {
        rawParsed <- cur.as[InitPositionRaw].map(raw => raw.copy(`type` = raw.`type`.toUpperCase))
        initPosition <- rawParsed match {
                          case InitPositionRaw("TRIM_HORIZON", _) =>
                            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON).asRight
                          case InitPositionRaw("LATEST", _) =>
                            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST).asRight
                          case InitPositionRaw("AT_TIMESTAMP", Some(timestamp)) =>
                            InitialPositionInStreamExtended.newInitialPositionAtTimestamp(Date.from(timestamp)).asRight
                          case other =>
                            DecodingFailure(
                              s"Initial position $other is not supported. Possible types are TRIM_HORIZON, LATEST and AT_TIMESTAMP (must provide timestamp field)",
                              cur.history
                            ).asLeft
                        }
      } yield initPosition
    }

  sealed trait Retrieval

  object Retrieval {
    case class Polling(maxRecords: Int) extends Retrieval

    case object FanOut extends Retrieval

    case class RetrievalRaw(`type`: String, maxRecords: Option[Int])
    implicit val retrievalRawDecoder: Decoder[RetrievalRaw] = deriveConfiguredDecoder[RetrievalRaw]

    implicit val retrievalDecoder: Decoder[Retrieval] =
      Decoder.instance { cur =>
        for {
          rawParsed <- cur.as[RetrievalRaw].map(raw => raw.copy(`type` = raw.`type`.toUpperCase))
          retrieval <- rawParsed match {
                         case RetrievalRaw("POLLING", Some(maxRecords)) =>
                           Polling(maxRecords).asRight
                         case RetrievalRaw("FANOUT", _) =>
                           FanOut.asRight
                         case other =>
                           DecodingFailure(
                             s"Retrieval mode $other is not supported. Possible types are FanOut and Polling (must provide maxRecords field)",
                             cur.history
                           ).asLeft
                       }
        } yield retrieval
      }
  }

  implicit val decoder: Decoder[KinesisSourceConfig] = deriveConfiguredDecoder[KinesisSourceConfig]
}
