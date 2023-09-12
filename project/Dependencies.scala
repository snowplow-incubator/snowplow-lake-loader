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
    val betterMonadicFor = "0.3.1"

    // Streams
    val fs2Kafka = "3.0.1"
    val pubsub   = "1.123.17"

    // Spark
    val spark          = "3.4.1"
    val delta          = "2.4.0"
    val iceberg        = "1.3.1"
    val hadoop         = "3.3.6"
    val gcsConnector   = "hadoop3-2.2.17"
    val biglakeIceberg = "0.1.0"
    val hiveCommon     = "3.1.3"

    // java
    val slf4j    = "2.0.7"
    val azureSdk = "1.9.1"
    val sentry   = "6.25.2"

    // Snowplow
    val schemaDdl  = "0.21.0-M1"
    val badrows    = "2.2.0"
    val igluClient = "3.0.0"
    val tracker    = "2.0.0"

    // Transitive overrides
    val protobuf     = "3.24.2"
    val snappy       = "1.1.10.2"
    val thrift       = "0.18.1"
    val jackson      = "2.14.2"

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
  val betterMonadicFor  = "com.olegpy"                 %% "better-monadic-for"         % V.betterMonadicFor

  // streams
  val fs2Kafka          = "com.github.fd4s"            %% "fs2-kafka"                  % V.fs2Kafka
  val pubsub            = "com.google.cloud"           % "google-cloud-pubsub"         % V.pubsub

  // spark and hadoop
  val sparkCore      = "org.apache.spark"   %% "spark-core"                 % V.spark
  val sparkSql       = "org.apache.spark"   %% "spark-sql"                  % V.spark
  val delta          = "io.delta"           %% "delta-core"                 % V.delta
  val iceberg        = "org.apache.iceberg" %% "iceberg-spark-runtime-3.4"  % V.iceberg
  val hadoopClient   = "org.apache.hadoop"           %  "hadoop-client-runtime" % V.hadoop
  val hadoopAzure    = "org.apache.hadoop"           %  "hadoop-azure"          % V.hadoop
  val gcsConnector   = "com.google.cloud.bigdataoss" % "gcs-connector"          % V.gcsConnector
  val hiveCommon     = ("org.apache.hive"            % "hive-common"            % V.hiveCommon)
    .exclude("com.github.joshelser", "dropwizard-metrics-hadoop-metrics2-reporter")
  
  // java
  val slf4j             = "org.slf4j"                  % "slf4j-simple"                % V.slf4j
  val azureIdentity     = "com.azure"                  % "azure-identity"              % V.azureSdk
  val sentry            = "io.sentry"                  % "sentry"                      % V.sentry

  // transitive overrides
  val protobuf          = "com.google.protobuf"        % "protobuf-java"                      % V.protobuf
  val snappy            = "org.xerial.snappy"          % "snappy-java"                        % V.snappy
  val hadoopYarn        = "org.apache.hadoop"          % "hadoop-yarn-server-resourcemanager" % V.hadoop
  val thrift            = "org.apache.thrift"          % "libthrift"                          % V.thrift
  val jackson           = "com.fasterxml.jackson.core" % "jackson-databind"                   % V.jackson

  // snowplow: Note jackson-databind 2.14.x is incompatible with Spark
  val badrows           = "com.snowplowanalytics"      %% "snowplow-badrows"                      % V.badrows
  val tracker           = "com.snowplowanalytics"      %% "snowplow-scala-tracker-core"           % V.tracker
  val trackerEmit       = "com.snowplowanalytics"      %% "snowplow-scala-tracker-emitter-http4s" % V.tracker
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
    circeGeneric,
    snappy
  )

  val pubsubDependencies = Seq(
    pubsub,
    circeConfig,
    circeGeneric
  )

  val loadersCommonDependencies = Seq(
    cats,
    catsEffectKernel,
    schemaDdl,
    badrows,
    circeConfig,
    fs2,
    igluClient,
    log4cats,
    tracker,
    trackerEmit,
    specs2,
    catsEffectSpecs2,
    slf4j % Test,
    jackson % Test // only needed because we excluded jackson because of the spark issue
  )

  val coreDependencies = Seq(
    catsRetry,
    sparkCore,
    sparkSql,
    igluClientHttp4s,
    blazeClient,
    decline,
    sentry,
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
    slf4j % Runtime,
    protobuf % Runtime
  )

  val azureDependencies = Seq(
    azureIdentity,
    hadoopAzure
  ) ++ commonRuntimeDependencies

  val gcpDependencies = Seq(
    gcsConnector % Runtime,
  ) ++ commonRuntimeDependencies

  val biglakeDependencies = Seq(
    hiveCommon % Runtime,
    hadoopYarn % Runtime,
    thrift % Runtime,
  )

  val commonExclusions = Seq(
    ExclusionRule(organization = "org.apache.zookeeper", name = "zookeeper"),
    ExclusionRule(organization = "org.eclipse.jetty", name = "jetty-client"),
    ExclusionRule(organization = "org.eclipse.jetty", name = "jetty-server"),
    ExclusionRule(organization = "org.eclipse.jetty", name = "jetty-http"),
    ExclusionRule(organization = "org.eclipse.jetty", name = "jetty-webapp"),
    ExclusionRule(organization = "org.eclipse.jetty", name = "jetty-webapp"),
    ExclusionRule(organization = "org.apache.kerby"),
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-yarn-server-applicationhistoryservice"),
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-yarn-server-common"),
    ExclusionRule(organization = "org.apache.ivy", name = "ivy"),
    ExclusionRule(organization = "org.apache.orc"),
  )

}
