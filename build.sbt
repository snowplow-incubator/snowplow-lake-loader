/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

lazy val root = project
  .in(file("."))
  .aggregate(
    core,
    azure,
    gcp,
    aws
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

lazy val aws: Project = project
  .in(file("modules/aws"))
  .settings(BuildSettings.awsSettings)
  .settings(libraryDependencies ++= Dependencies.awsDependencies)
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
