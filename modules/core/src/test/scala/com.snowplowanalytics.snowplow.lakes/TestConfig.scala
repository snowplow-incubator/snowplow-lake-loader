/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
      case Left(e)   => throw new RuntimeException("Could not load default config for testing", e)
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
