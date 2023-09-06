/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

lazy val root = project
  .in(file("."))
  .aggregate(
    streams,
    kafka,
    pubsub,
    loadersCommon,
    core,
    azure,
    gcp
  )

/* Common Snowplow internal modules, to become separate library */

lazy val streams: Project = project
  .in(file("snowplow-common-internal/streams-core"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.streamsDependencies)

lazy val kinesis: Project = project
  .in(file("snowplow-common-internal/kinesis"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.kinesisDependencies)
  .dependsOn(streams)
  .settings(
    Defaults.itSettings,
    /**
     * AWS_REGION=eu-central-1 is detected by the lib & integration test suite which follows the
     * same region resolution mechanism as the lib
     */
    IntegrationTest / envVars := Map("AWS_REGION" -> "eu-central-1")
  )
  .configs(IntegrationTest)

lazy val kafka: Project = project
  .in(file("snowplow-common-internal/kafka"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .dependsOn(streams)

lazy val pubsub: Project = project
  .in(file("snowplow-common-internal/pubsub"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.pubsubDependencies)
  .dependsOn(streams)

lazy val loadersCommon: Project = project
  .in(file("snowplow-common-internal/loaders-common"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.loadersCommonDependencies)

/* End of common Snowplow internal modules */

/* This app */

lazy val core: Project = project
  .in(file("modules/core"))
  .settings(BuildSettings.commonSettings ++ BuildSettings.logSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(loadersCommon, streams)

lazy val azure: Project = project
  .in(file("modules/azure"))
  .settings(BuildSettings.azureSettings)
  .settings(libraryDependencies ++= Dependencies.azureDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(core, kafka)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val gcp: Project = project
  .in(file("modules/gcp"))
  .settings(BuildSettings.gcpSettings)
  .settings(libraryDependencies ++= Dependencies.gcpDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(core, pubsub)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

// Temporarily, separate out biglake support into its own project.
// Because the biglake shaded jar adds CVEs which are difficult to remove from the build.
lazy val gcpWithBiglake: Project = project
  .in(file("modules/gcpWithBiglake"))
  .settings(BuildSettings.gcpSettings)
  .settings(BuildSettings.biglakeSettings)
  .settings(sourceDirectory := (gcp / sourceDirectory).value)
  .settings(libraryDependencies ++= Dependencies.gcpDependencies)
  .settings(libraryDependencies ++= Dependencies.biglakeDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(core, pubsub)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

ThisBuild / fork := true
