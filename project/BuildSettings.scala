/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

// SBT
import sbt._
import sbt.io.IO
import Keys._

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtdynver.DynVerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._

import scala.sys.process._

object BuildSettings {

  lazy val commonSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.12.18",
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false,
    scalacOptions += "-Ywarn-macros:after",
    addCompilerPlugin(Dependencies.betterMonadicFor),
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-", // to be compatible with docker

    Compile / resourceGenerators += Def.task {
      val license = (Compile / resourceManaged).value / "META-INF" / "LICENSE"
      IO.copyFile(file("LICENSE.md"), license)
      Seq(license)
    }.taskValue
  )

  lazy val logSettings = Seq(
    excludeDependencies ++= Seq(
      ExclusionRule("org.apache.logging.log4j", "log4j-slf4j2-impl")
    )
  )

  lazy val appSettings = Seq(
    buildInfoKeys := Seq[BuildInfoKey](dockerAlias, name, version),
    buildInfoPackage := "com.snowplowanalytics.snowplow.lakes",
    buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.runtime.AppInfo")
  ) ++ commonSettings ++ logSettings

  lazy val awsSettings = appSettings ++ Seq(
    name := "lake-loader-aws",
    buildInfoKeys += BuildInfoKey("cloud" -> "AWS")
  )

  lazy val azureSettings = appSettings ++ Seq(
    name := "lake-loader-azure",
    buildInfoKeys += BuildInfoKey("cloud" -> "Azure")
  )

  lazy val downloadUnmanagedJars = taskKey[Unit]("Downloads unmanaged Jars")

  lazy val gcpSettings = appSettings ++ Seq(
    name := "lake-loader-gcp",
    buildInfoKeys += BuildInfoKey("cloud" -> "GCP")
  )

  lazy val biglakeSettings = Seq(
    downloadUnmanagedJars := {
      val libDir = baseDirectory.value / "lib"
      IO.createDirectory(libDir)
      val file = libDir / "biglake-catalog-iceberg1.2.0-0.1.0-with-dependencies.jar"
      if (!file.exists) {
        url(
          "https://storage.googleapis.com/storage/v1/b/spark-lib/o/biglake%2Fbiglake-catalog-iceberg1.2.0-0.1.0-with-dependencies.jar?alt=media"
        ) #> file !
      }
    },
    Compile / compile := ((Compile / compile) dependsOn downloadUnmanagedJars).value,
    dockerAlias := dockerAlias.value.copy(tag = dockerAlias.value.tag.map(t => s"$t-biglake"))
  )

}
