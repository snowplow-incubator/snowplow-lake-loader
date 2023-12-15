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

import fs2.io.file.Path

object TestConfig {

  sealed trait Target
  case object Delta extends Target
  case object Hudi extends Target
  case object Iceberg extends Target

  /** Provides an app Config using defaults provided by our standard reference.conf */
  def defaults(target: Target, tmpDir: Path): AnyConfig =
    ConfigFactory
      .load(ConfigFactory.parseString(configOverrides(target, tmpDir)))
      .as[Config[Option[Unit], Option[Unit]]] match {
      case Right(ok) => ok
      case Left(e)   => throw new RuntimeException("Could not load default config for testing", e)
    }

  private def configOverrides(target: TestConfig.Target, tmpDir: Path): String = {
    val location = (tmpDir / "events").toNioPath.toUri
    target match {
      case Delta =>
        s"""
        output.good: {
          type: "Delta"
          location: "$location"
        }
        """
      case Hudi =>
        s"""
        output.good: {
          type: "Hudi"
          location: "$location"
        }
        """
      case Iceberg =>
        s"""
        output.good: {
          type: "IcebergHadoop"
          database: "test"
          table: "events"
          location: "${tmpDir.toNioPath.toUri}"
        }
        """
    }
  }

}
