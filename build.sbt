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
  .settings(BuildSettings.igluTestSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)

lazy val azure: Project = project
  .in(file("modules/azure"))
  .settings(BuildSettings.azureSettings)
  .settings(libraryDependencies ++= Dependencies.azureDependencies ++ Dependencies.icebergDeltaRuntimeDependencies)
  .dependsOn(core, deltaIceberg)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val gcp: Project = project
  .in(file("modules/gcp"))
  .settings(BuildSettings.gcpSettings)
  .settings(libraryDependencies ++= Dependencies.gcpDependencies ++ Dependencies.icebergDeltaRuntimeDependencies)
  .dependsOn(core, deltaIceberg)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val aws: Project = project
  .in(file("modules/aws"))
  .settings(BuildSettings.awsSettings)
  .settings(libraryDependencies ++= Dependencies.awsDependencies ++ Dependencies.icebergDeltaRuntimeDependencies)
  .dependsOn(core, deltaIceberg)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

/** Packaging: Extra runtime dependencies for alternative assets */

lazy val hudi: Project = project
  .in(file("packaging/hudi"))
  .settings(BuildSettings.commonSettings)
  .settings(BuildSettings.igluTestSettings)
  .settings(libraryDependencies ++= Dependencies.hudiDependencies)
  .dependsOn(core % "test->test")

lazy val biglake: Project = project
  .in(file("packaging/biglake"))
  .settings(BuildSettings.commonSettings ++ BuildSettings.biglakeSettings)
  .settings(libraryDependencies ++= Dependencies.biglakeDependencies)

lazy val deltaIceberg: Project = project
  .in(file("packaging/delta-iceberg"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.icebergDeltaRuntimeDependencies)

/**
 * Packaging: Alternative assets
 *
 * These exist because there are some depenencies we don't want to put in the regular assets. Helps
 * with CVE management.
 */

lazy val awsHudi: Project = project
  .in(file("modules/aws"))
  .withId("awsHudi")
  .settings(BuildSettings.awsSettings ++ BuildSettings.hudiAppSettings)
  .settings(target := (hudi / target).value / "aws")
  .settings(libraryDependencies ++= Dependencies.awsDependencies ++ Dependencies.hudiAwsDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .dependsOn(hudi % "runtime->runtime;compile->compile")

lazy val gcpHudi: Project = project
  .in(file("modules/gcp"))
  .withId("gcpHudi")
  .settings(BuildSettings.gcpSettings ++ BuildSettings.hudiAppSettings)
  .settings(target := (hudi / target).value / "gcp")
  .settings(libraryDependencies ++= Dependencies.gcpDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .dependsOn(hudi % "runtime->runtime;compile->compile")

lazy val azureHudi: Project = project
  .in(file("modules/azure"))
  .withId("azureHudi")
  .settings(BuildSettings.azureSettings ++ BuildSettings.hudiAppSettings)
  .settings(target := (hudi / target).value / "azure")
  .settings(libraryDependencies ++= Dependencies.azureDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .dependsOn(hudi % "runtime->runtime;compile->compile")

lazy val gcpBiglake: Project = gcp
  .withId("gcpBiglake")
  .settings(target := (biglake / target).value / "gcp")
  .settings(BuildSettings.biglakeAppSettings)
  .dependsOn(biglake % "runtime->runtime")

ThisBuild / fork := true
