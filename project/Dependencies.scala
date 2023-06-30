/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
import sbt._

object Dependencies {

  object V {
    // Scala
    val cats             = "2.9.0"
    val catsEffect       = "3.5.0"
    val catsRetry        = "3.1.0"
    val fs2              = "3.7.0"
    val log4cats         = "2.6.0"
    val http4s           = "0.23.15"
    val decline          = "2.4.1"
    val circe            = "0.14.1"
    val circeConfig      = "0.10.0"

    // Streams
    val fs2Kafka = "3.0.1"

    // Spark
    val spark   = "3.4.0"
    val delta   = "2.4.0"
    val iceberg = "1.3.0"
    val hadoop  = "3.3.5"

    // java
    val slf4j    = "2.0.7"
    val azureSdk = "1.9.1"

    // Snowplow
    val schemaDdl  = "0.18.2"
    val badrows    = "2.2.0"
    val igluClient = "3.0.0"

    // tests
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"

  }

  val catsEffectKernel  = "org.typelevel"              %% "cats-effect-kernel"         % V.catsEffect
  val cats              = "org.typelevel"              %% "cats-core"                  % V.cats
  val fs2               = "co.fs2"                     %% "fs2-core"                   % V.fs2
  val log4cats          = "org.typelevel"              %% "log4cats-slf4j"             % V.log4cats
  val catsRetry         = "com.github.cb372"           %% "cats-retry"                 % V.catsRetry
  val blazeClient       = "org.http4s"                 %% "http4s-blaze-client"        % V.http4s
  val decline           = "com.monovore"               %% "decline-effect"             % V.decline
  val circeConfig       = "io.circe"                   %% "circe-config"               % V.circeConfig
  val circeGeneric      = "io.circe"                   %% "circe-generic"              % V.circe
  val circeGenericExtra = "io.circe"                   %% "circe-generic-extras"       % V.circe

  // streams
  val fs2Kafka          = "com.github.fd4s"            %% "fs2-kafka"                  % V.fs2Kafka

  // spark and hadoop
  val sparkCore    = "org.apache.spark"   %% "spark-core"                 % V.spark
  val sparkSql     = "org.apache.spark"   %% "spark-sql"                  % V.spark
  val delta        = "io.delta"           %% "delta-core"                 % V.delta
  val iceberg      = "org.apache.iceberg" %% "iceberg-spark-runtime-3.4"  % V.iceberg
  val hadoopClient = "org.apache.hadoop"  %  "hadoop-client"              % V.hadoop
  val hadoopAzure  = "org.apache.hadoop"  %  "hadoop-azure"               % V.hadoop
  
  // java
  val slf4j             = "org.slf4j"                  % "slf4j-simple"                % V.slf4j
  val azureIdentity     = "com.azure"                  % "azure-identity"              % V.azureSdk

  // snowplow: Note jackson-databind 2.14.x is incompatible with Spark
  val badrows           = "com.snowplowanalytics"      %% "snowplow-badrows"         % V.badrows
  val schemaDdl         = ("com.snowplowanalytics"      %% "schema-ddl"               % V.schemaDdl)
    .exclude("com.fasterxml.jackson.core", "jackson-databind")
  val igluClient        = ("com.snowplowanalytics"      %% "iglu-scala-client"        % V.igluClient)
    .exclude("com.fasterxml.jackson.core", "jackson-databind")
  val igluClientHttp4s  = ("com.snowplowanalytics"      %% "iglu-scala-client-http4s" % V.igluClient)
    .exclude("com.fasterxml.jackson.core", "jackson-databind")

  // tests
  val specs2            = "org.specs2"                 %% "specs2-core"                % V.specs2            % Test
  val catsEffectTestkit = "org.typelevel"              %% "cats-effect-testkit"        % V.catsEffect        % Test
  val catsEffectSpecs2  = "org.typelevel"              %% "cats-effect-testing-specs2" % V.catsEffectSpecs2  % Test


  val streamsDependencies = Seq(
    cats,
    catsEffectKernel,
    catsRetry,
    fs2,
    log4cats,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    slf4j % Provided
  )

  val kafkaDependencies = Seq(
    fs2Kafka,
    circeConfig,
    circeGeneric
  )

  val coreDependencies = Seq(
    catsRetry,
    sparkCore,
    sparkSql,
    schemaDdl,
    badrows,
    igluClient,
    igluClientHttp4s,
    blazeClient,
    decline,
    circeConfig,
    circeGenericExtra,
    delta,
    specs2,
    catsEffectSpecs2,
    slf4j % Test
  )

  val commonRuntimeDependencies = Seq(
    delta % Runtime,
    iceberg % Runtime,
    hadoopClient % Runtime,
    slf4j % Runtime
  )

  val azureDependencies = Seq(
    azureIdentity,
    hadoopAzure
  ) ++ commonRuntimeDependencies

}
