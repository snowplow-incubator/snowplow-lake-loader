/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources

import cats.implicits._
import io.circe._
import io.circe.generic.semiauto.deriveDecoder

import java.net.URI
import java.time.Instant

case class KinesisSourceConfig(
  appName: String,
  streamName: String,
  region: Option[String],
  initialPosition: KinesisSourceConfig.InitPosition,
  retrievalMode: KinesisSourceConfig.Retrieval,
  bufferSize: Int,
  customEndpoint: Option[URI],
  dynamodbCustomEndpoint: Option[URI],
  cloudwatchCustomEndpoint: Option[URI],
  cloudwatch: Boolean
)

object KinesisSourceConfig {
  sealed trait InitPosition

  object InitPosition {
    case object Latest extends InitPosition

    case object TrimHorizon extends InitPosition

    case class AtTimestamp(timestamp: Instant) extends InitPosition

    case class InitPositionRaw(`type`: String, timestamp: Option[Instant])

    implicit val initPositionRawDecoder: Decoder[InitPositionRaw] = deriveDecoder[InitPositionRaw]

    implicit val initPositionDecoder: Decoder[InitPosition] =
      Decoder.instance { cur =>
        for {
          rawParsed <- cur.as[InitPositionRaw].map(raw => raw.copy(`type` = raw.`type`.toUpperCase))
          initPosition <- rawParsed match {
                            case InitPositionRaw("TRIM_HORIZON", _) =>
                              TrimHorizon.asRight
                            case InitPositionRaw("LATEST", _) =>
                              Latest.asRight
                            case InitPositionRaw("AT_TIMESTAMP", Some(timestamp)) =>
                              AtTimestamp(timestamp).asRight
                            case other =>
                              DecodingFailure(
                                s"Initial position $other is not supported. Possible types are TRIM_HORIZON, LATEST and AT_TIMESTAMP (must provide timestamp field)",
                                cur.history
                              ).asLeft
                          }
        } yield initPosition
      }
  }

  sealed trait Retrieval

  object Retrieval {
    case class Polling(maxRecords: Int) extends Retrieval

    case object FanOut extends Retrieval

    case class RetrievalRaw(`type`: String, maxRecords: Option[Int])

    implicit val retrievalRawDecoder: Decoder[RetrievalRaw] = deriveDecoder[RetrievalRaw]

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

  implicit def decoder: Decoder[KinesisSourceConfig] = deriveDecoder[KinesisSourceConfig]
}
