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

lazy val hudiAws: Project = project
  .in(file("packaging/hudi-aws"))
  .settings(BuildSettings.hudiAwsPackagingSettings)
  .settings(libraryDependencies ++= Dependencies.hudiAwsDependencies)

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
  .dependsOn(hudi % "runtime->runtime", hudiAws)

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
  .dependsOn(biglake % "runtime->runtime")

ThisBuild / fork := true
