/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub

import io.circe.Decoder
import cats.implicits._

import com.snowplowanalytics.snowplow.sources.pubsub.v2.PubsubSourceConfigV2

/**
 * Allows experimental support for the V2 source, while loading the V1 source by default
 *
 * Users can select the v2 Source by setting `"version": "v2"` in the hocon file
 */
sealed trait PubsubSourceAlternative

object PubsubSourceAlternative {
  case class V1(config: PubsubSourceConfig) extends PubsubSourceAlternative
  case class V2(config: PubsubSourceConfigV2) extends PubsubSourceAlternative

  implicit def decoder: Decoder[PubsubSourceAlternative] = Decoder.decodeJsonObject.flatMap {
    case obj if obj("version").flatMap(_.asString) === Some("v2") =>
      implicitly[Decoder[PubsubSourceConfigV2]].map(V2(_))
    case _ =>
      implicitly[Decoder[PubsubSourceConfig]].map(V1(_))
  }
}
