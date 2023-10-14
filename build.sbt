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
    core,
    azure,
    gcp
  )

lazy val core: Project = project
  .in(file("modules/core"))
  .settings(BuildSettings.commonSettings ++ BuildSettings.logSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)

lazy val azure: Project = project
  .in(file("modules/azure"))
  .settings(BuildSettings.azureSettings)
  .settings(libraryDependencies ++= Dependencies.azureDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val gcp: Project = project
  .in(file("modules/gcp"))
  .settings(BuildSettings.gcpSettings)
  .settings(libraryDependencies ++= Dependencies.gcpDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(core)
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
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

ThisBuild / fork := true
