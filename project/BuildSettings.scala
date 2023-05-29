/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
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

  lazy val appSettings = Seq(
      buildInfoKeys := Seq[BuildInfoKey](dockerAlias, name, version),
      buildInfoPackage := "com.snowplowanalytics.snowplow.lakes",
      excludeDependencies ++= Seq(
        ExclusionRule("org.apache.logging.log4j", "log4j-slf4j2-impl")
      )
    )

  lazy val azureSettings = Seq(
    name := "lake-loader-azure",
  ) ++ appSettings

}
