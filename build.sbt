/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

lazy val root = project.in(file("."))
  .aggregate(
    streams,
    kafka,
    core
  )

lazy val streams: Project = project
  .in(file("modules/streams"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.streamsDependencies)

lazy val kafka: Project = project
  .in(file("modules/kafka"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .dependsOn(streams)

lazy val core: Project = project
  .in(file("modules/core"))
  .settings(BuildSettings.commonSettings ++ BuildSettings.logSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)
  .dependsOn(streams)

lazy val azure: Project = project
  .in(file("modules/azure"))
  .settings(BuildSettings.commonSettings)
  .settings(BuildSettings.azureSettings)
  .settings(libraryDependencies ++= Dependencies.azureDependencies)
  .dependsOn(core, kafka)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

fork := true
