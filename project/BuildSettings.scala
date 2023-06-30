/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

// SBT
import sbt._
import Keys._

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._

object BuildSettings {

  lazy val commonSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.13.10",
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false,
    scalacOptions += "-Ywarn-macros:after"
  )

  lazy val logSettings = Seq(
    excludeDependencies ++= Seq(
      ExclusionRule("org.apache.logging.log4j", "log4j-slf4j2-impl")
    )
  )

  lazy val appSettings = Seq(
      buildInfoKeys := Seq[BuildInfoKey](dockerAlias, name, version),
      buildInfoPackage := "com.snowplowanalytics.snowplow.lakes"
    ) ++ logSettings

  lazy val azureSettings = Seq(
    name := "lake-loader-azure",
  ) ++ appSettings

}
