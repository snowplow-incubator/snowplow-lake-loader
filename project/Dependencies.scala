/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
import sbt._

object Dependencies {

  object V {
    // Scala
    val catsEffect       = "3.5.0"
    val catsRetry        = "3.1.0"
    val decline          = "2.4.1"
    val circe            = "0.14.1"
    val http4s           = "0.23.15"
    val betterMonadicFor = "0.3.1"

    // Spark
    val spark          = "3.4.1"
    val delta          = "2.4.0"
    val hudi           = "0.14.0"
    val iceberg        = "1.3.1"
    val hadoop         = "3.3.6"
    val gcsConnector   = "hadoop3-2.2.17"
    val biglakeIceberg = "0.1.0"
    val hive           = "3.1.3"

    // java
    val slf4j    = "2.0.7"
    val azureSdk = "1.11.1"
    val sentry   = "6.25.2"
    val awsSdk1  = "1.12.646"
    val awsSdk2  = "2.20.43" // Match common-streams

    // Snowplow
    val streams    = "0.5.0"
    val igluClient = "3.0.0"

    // Transitive overrides
    val protobuf = "3.25.1"
    val snappy   = "1.1.10.5"
    val thrift   = "0.18.1"
    val netty    = "4.1.104.Final"

    // tests
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"

  }

  val catsRetry         = "com.github.cb372" %% "cats-retry"           % V.catsRetry
  val blazeClient       = "org.http4s"       %% "http4s-blaze-client"  % V.http4s
  val decline           = "com.monovore"     %% "decline-effect"       % V.decline
  val circeGenericExtra = "io.circe"         %% "circe-generic-extras" % V.circe
  val betterMonadicFor  = "com.olegpy"       %% "better-monadic-for"   % V.betterMonadicFor

  // spark and hadoop
  val sparkCore    = "org.apache.spark"           %% "spark-core"                % V.spark
  val sparkSql     = "org.apache.spark"           %% "spark-sql"                 % V.spark
  val sparkHive    = "org.apache.spark"           %% "spark-hive"                % V.spark
  val delta        = "io.delta"                   %% "delta-core"                % V.delta
  val hudi         = "org.apache.hudi"            %% "hudi-spark3.4-bundle"      % V.hudi
  val iceberg      = "org.apache.iceberg"         %% "iceberg-spark-runtime-3.4" % V.iceberg
  val hadoopClient = "org.apache.hadoop"           % "hadoop-client-runtime"     % V.hadoop
  val hadoopAzure  = "org.apache.hadoop"           % "hadoop-azure"              % V.hadoop
  val hadoopAws    = "org.apache.hadoop"           % "hadoop-aws"                % V.hadoop
  val gcsConnector = "com.google.cloud.bigdataoss" % "gcs-connector"             % V.gcsConnector
  val hiveCommon   = "org.apache.hive"             % "hive-common"               % V.hive

  // java
  val slf4j         = "org.slf4j"              % "slf4j-simple"   % V.slf4j
  val azureIdentity = "com.azure"              % "azure-identity" % V.azureSdk
  val sentry        = "io.sentry"              % "sentry"         % V.sentry
  val awsGlue       = "software.amazon.awssdk" % "glue"           % V.awsSdk2 % Runtime
  val awsS3         = "software.amazon.awssdk" % "s3"             % V.awsSdk2 % Runtime
  val awsSts        = "software.amazon.awssdk" % "sts"            % V.awsSdk2 % Runtime

  // transitive overrides
  val protobuf   = "com.google.protobuf" % "protobuf-java"                      % V.protobuf
  val snappy     = "org.xerial.snappy"   % "snappy-java"                        % V.snappy
  val hadoopYarn = "org.apache.hadoop"   % "hadoop-yarn-server-resourcemanager" % V.hadoop
  val thrift     = "org.apache.thrift"   % "libthrift"                          % V.thrift
  val netty      = "io.netty"            % "netty-all"                          % V.netty
  val awsBundle  = "com.amazonaws"       % "aws-java-sdk-bundle"                % V.awsSdk1

  val streamsCore      = "com.snowplowanalytics" %% "streams-core"             % V.streams
  val kinesis          = "com.snowplowanalytics" %% "kinesis"                  % V.streams
  val kafka            = "com.snowplowanalytics" %% "kafka"                    % V.streams
  val pubsub           = "com.snowplowanalytics" %% "pubsub"                   % V.streams
  val loaders          = "com.snowplowanalytics" %% "loaders-common"           % V.streams
  val runtime          = "com.snowplowanalytics" %% "runtime-common"           % V.streams
  val igluClientHttp4s = "com.snowplowanalytics" %% "iglu-scala-client-http4s" % V.igluClient

  // tests
  val specs2            = "org.specs2"    %% "specs2-core"                % V.specs2           % Test
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit"        % V.catsEffect       % Test
  val catsEffectSpecs2  = "org.typelevel" %% "cats-effect-testing-specs2" % V.catsEffectSpecs2 % Test

  val commonRuntimeDependencies = Seq(
    delta        % Runtime,
    iceberg      % Runtime,
    hadoopClient % Runtime,
    slf4j        % Runtime,
    protobuf     % Runtime,
    netty        % Runtime,
    snappy       % Runtime
  )

  val coreDependencies = Seq(
    streamsCore,
    loaders,
    runtime,
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
    catsEffectTestkit,
    slf4j % Test
  ) ++ commonRuntimeDependencies

  val awsDependencies = Seq(
    kinesis,
    hadoopAws,
    awsBundle, // Dependency on aws sdk v1 will likely be removed in the next release of hadoop-aws
    awsGlue,
    awsS3,
    awsSts
  ) ++ commonRuntimeDependencies

  val azureDependencies = Seq(
    kafka,
    azureIdentity,
    hadoopAzure
  ) ++ commonRuntimeDependencies

  val gcpDependencies = Seq(
    pubsub,
    gcsConnector
  ) ++ commonRuntimeDependencies

  val biglakeDependencies = Seq(
    hiveCommon % Runtime,
    hadoopYarn % Runtime,
    thrift     % Runtime
  )

  val hudiDependencies = Seq(
    hudi      % Runtime,
    sparkHive % Runtime
  )

  val commonExclusions = Seq(
    ExclusionRule(organization = "org.apache.zookeeper", name     = "zookeeper"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-client"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-server"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-http"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-webapp"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-webapp"),
    ExclusionRule(organization = "org.apache.kerby"),
    ExclusionRule(organization = "org.apache.hadoop", name        = "hadoop-yarn-server-applicationhistoryservice"),
    ExclusionRule(organization = "org.apache.hadoop", name        = "hadoop-yarn-server-common"),
    ExclusionRule(organization = "org.apache.ivy", name           = "ivy"),
    ExclusionRule(organization = "com.github.joshelser", name     = "dropwizard-metrics-hadoop-metrics2-reporter"),
    ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-slf4j2-impl")
  )

}
