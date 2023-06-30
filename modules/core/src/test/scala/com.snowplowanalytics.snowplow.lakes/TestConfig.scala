/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes

import com.typesafe.config.ConfigFactory
import cats.implicits._
import io.circe.config.syntax._

object TestConfig {

  /** Provides an app Config using defaults provided by our standard reference.conf */
  def defaults: AnyConfig =
    ConfigFactory
      .load(fallbacks)
      .as[Config[Option[Unit], Option[Unit]]] match {
      case Right(ok) => ok
      case Left(e) => throw new RuntimeException("Could not load default config for testing", e)
    }

  private def fallbacks = ConfigFactory.parseString("""
    output: {
      good: {
        type: Delta
        location: "file:///tmp/lake-loader-test/events"
      }
    }
  """)
}
