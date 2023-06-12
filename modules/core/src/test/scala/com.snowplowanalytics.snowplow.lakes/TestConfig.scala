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
