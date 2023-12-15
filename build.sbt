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
    gcp,
    aws,
    hudi,
    biglake,
    awsHudi,
    gcpHudi,
    azureHudi,
    gcpBiglake
  )

lazy val core: Project = project
  .in(file("modules/core"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)

lazy val azure: Project = project
  .in(file("modules/azure"))
  .settings(BuildSettings.azureSettings)
  .settings(libraryDependencies ++= Dependencies.azureDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val gcp: Project = project
  .in(file("modules/gcp"))
  .settings(BuildSettings.gcpSettings)
  .settings(libraryDependencies ++= Dependencies.gcpDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val aws: Project = project
  .in(file("modules/aws"))
  .settings(BuildSettings.awsSettings)
  .settings(libraryDependencies ++= Dependencies.awsDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

/** Packaging: Extra runtime dependencies for alternative assets * */

lazy val hudi: Project = project
  .in(file("packaging/hudi"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.hudiDependencies)
  .dependsOn(core % "test->test")

lazy val biglake: Project = project
  .in(file("packaging/biglake"))
  .settings(BuildSettings.commonSettings ++ BuildSettings.biglakeSettings)
  .settings(libraryDependencies ++= Dependencies.biglakeDependencies)

/**
 * Packaging: Alternative assets
 *
 * These exist because there are some depenencies we don't want to put in the regular assets. Helps
 * with CVE management.
 */

lazy val awsHudi: Project = aws
  .withId("awsHudi")
  .settings(target := (hudi / target).value / "aws")
  .settings(BuildSettings.hudiAppSettings)
  .dependsOn(hudi % "runtime->runtime")

lazy val gcpHudi: Project = gcp
  .withId("gcpHudi")
  .settings(target := (hudi / target).value / "gcp")
  .settings(BuildSettings.hudiAppSettings)
  .dependsOn(hudi % "runtime->runtime")

lazy val azureHudi: Project = azure
  .withId("azureHudi")
  .settings(target := (hudi / target).value / "azure")
  .settings(BuildSettings.hudiAppSettings)
  .dependsOn(hudi % "runtime->runtime")

lazy val gcpBiglake: Project = gcp
  .withId("gcpBiglake")
  .settings(target := (biglake / target).value / "gcp")
  .settings(BuildSettings.biglakeAppSettings)
  .dependsOn(hudi % "runtime->runtime")

ThisBuild / fork := true
