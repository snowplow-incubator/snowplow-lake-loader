/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
import sbt._

object Dependencies {

  object V {
    object Spark {

      // A version of Spark which is compatible with the current version of Iceberg and Delta
      val forIcebergDelta      = "4.1.3"
      val forIcebergDeltaMinor = "4.1"
    }

    // Scala
    val catsEffect       = "3.5.4"
    val decline          = "2.4.1"
    val circe            = "0.14.4"
    val http4s           = "0.23.29"
    val betterMonadicFor = "0.3.1"

    // Spark
    val delta              = "4.3.1"
    val iceberg            = "1.11.0"
    val hadoop             = "3.5.0"
    val googleCloudStorage = "2.70.0"

    // java
    val slf4j       = "2.0.13"
    val azureSdk    = "1.18.4"
    val awsSdk1     = "1.12.777"
    val awsSdk2     = "2.42.23" // Match common-streams
    val awsRegistry = "1.1.20"
    val jsonSmart   = "2.5.2"

    // Snowplow
    val streams    = "0.26.0"
    val igluClient = "4.2.1"

    // Transitive overrides
    val kafka         = "3.9.2"
    val lz4           = "1.11.1"
    val log4jCore     = "2.25.4"
    val micrometer    = "1.16.6"
    val opentelemetry = "1.62.0"
    val netty         = "4.2.16.Final"
    val bouncyCastle  = "1.85"

    // tests
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"

  }

  val decline           = "com.monovore" %% "decline-effect"       % V.decline
  val circeGenericExtra = "io.circe"     %% "circe-generic-extras" % V.circe
  val betterMonadicFor  = "com.olegpy"   %% "better-monadic-for"   % V.betterMonadicFor

  object Spark {
    val coreForIcebergDelta = "org.apache.spark" %% "spark-core" % V.Spark.forIcebergDelta
    val sqlForIcebergDelta  = "org.apache.spark" %% "spark-sql"  % V.Spark.forIcebergDelta
  }

  // spark and hadoop
  val delta         = "io.delta"           %% s"delta-spark_${V.Spark.forIcebergDeltaMinor}"           % V.delta
  val deltaDynamodb = "io.delta"            % "delta-storage-s3-dynamodb"                              % V.delta
  val iceberg       = "org.apache.iceberg" %% s"iceberg-spark-runtime-${V.Spark.forIcebergDeltaMinor}" % V.iceberg
  val hadoopClient  = "org.apache.hadoop"   % "hadoop-client-runtime"                                  % V.hadoop
  val hadoopAzure   = "org.apache.hadoop"   % "hadoop-azure"                                           % V.hadoop
  val hadoopAws     = "org.apache.hadoop"   % "hadoop-aws"                                             % V.hadoop
  val hadoopGcp     = "org.apache.hadoop"   % "hadoop-gcp"                                             % V.hadoop
  // Iceberg's ResolvingFileIO maps gs:// to GCSFileIO: iceberg-spark-runtime bundles that class
  // but not the java-storage client behind it, which previously reached the classpath only as a
  // transitive dependency of the removed bigdataoss gcsio. Also provides the plain exception
  // classes GcpApp's triage matches for the Iceberg REST catalog path.
  val googleCloudStorage = "com.google.cloud" % "google-cloud-storage" % V.googleCloudStorage

  // java
  val slf4j         = "org.slf4j"              % "slf4j-simple"          % V.slf4j
  val azureIdentity = "com.azure"              % "azure-identity"        % V.azureSdk
  val awsGlue       = "software.amazon.awssdk" % "glue"                  % V.awsSdk2
  val awsS3         = "software.amazon.awssdk" % "s3"                    % V.awsSdk2
  val awsS3Transfer = "software.amazon.awssdk" % "s3-transfer-manager"   % V.awsSdk2
  val awsSts        = "software.amazon.awssdk" % "sts"                   % V.awsSdk2
  val awsKms        = "software.amazon.awssdk" % "kms"                   % V.awsSdk2
  val dynamodbSdk1  = "com.amazonaws"          % "aws-java-sdk-dynamodb" % V.awsSdk1
  val awsRegistry   = "software.amazon.glue"   % "schema-registry-serde" % V.awsRegistry
  val jsonSmart     = "net.minidev"            % "json-smart"            % V.jsonSmart

  // transitive overrides
  val kafkaClients  = "org.apache.kafka"         % "kafka-clients"     % V.kafka
  val lz4           = "at.yawk.lz4"              % "lz4-java"          % V.lz4
  val log4jCore     = "org.apache.logging.log4j" % "log4j-core"        % V.log4jCore
  val micrometer    = "io.micrometer"            % "micrometer-core"   % V.micrometer
  val opentelemetry = "io.opentelemetry"         % "opentelemetry-api" % V.opentelemetry
  // Spark pulls Netty's aggregator netty-all, which has a non-optional dependency on a
  // vulnerable bcprov-jdk18on 1.80; pin it to a patched version.
  val bouncyCastle = "org.bouncycastle" % "bcprov-jdk18on" % V.bouncyCastle

  // Bump the whole Netty family (pulled transitively by Spark and the AWS SDK) to a patched
  // version. Netty modules must share a version, so every module on the classpath is listed
  // and pinned together via highest-wins. netty-all is deliberately excluded: it is a classless
  // aggregator whose non-optional dependencies would drag in unused protocol modules (mqtt,
  // redis, rxtx, ...) if declared directly.
  private val nettyModules = Seq(
    "netty-buffer",
    "netty-codec",
    "netty-codec-base",
    "netty-codec-classes-quic",
    "netty-codec-compression",
    "netty-codec-dns",
    "netty-codec-http",
    "netty-codec-http2",
    "netty-codec-http3",
    "netty-codec-marshalling",
    "netty-codec-native-quic",
    "netty-codec-protobuf",
    "netty-codec-socks",
    "netty-common",
    "netty-handler",
    "netty-handler-proxy",
    "netty-resolver",
    "netty-resolver-dns",
    "netty-transport",
    "netty-transport-classes-epoll",
    "netty-transport-classes-io_uring",
    "netty-transport-classes-kqueue",
    "netty-transport-native-epoll",
    "netty-transport-native-io_uring",
    "netty-transport-native-kqueue",
    "netty-transport-native-unix-common"
  )

  val nettyDependencies: Seq[ModuleID] = nettyModules.map("io.netty" % _ % V.netty)

  // snowplow
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
    slf4j      % Runtime,
    micrometer % Runtime
  )

  val coreDependencies = Seq(
    streamsCore,
    loaders,
    runtime,
    delta,
    Spark.coreForIcebergDelta,
    Spark.sqlForIcebergDelta,
    iceberg,
    igluClientHttp4s,
    decline,
    circeGenericExtra,
    hadoopClient,
    lz4,
    log4jCore,
    bouncyCastle,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    slf4j % Test
  ) ++ commonRuntimeDependencies ++ nettyDependencies

  val awsDependencies = Seq(
    kinesis,
    hadoopAws.exclude("software.amazon.awssdk", "bundle"),
    awsS3,
    awsGlue,
    awsSts,
    dynamodbSdk1,
    awsKms        % Runtime,
    deltaDynamodb % Runtime,
    awsS3Transfer % Runtime
  ) ++ commonRuntimeDependencies

  val azureDependencies = Seq(
    kafka,
    azureIdentity,
    hadoopAzure,
    hadoopClient,
    jsonSmart,
    kafkaClients % Runtime,
    specs2
  ) ++ commonRuntimeDependencies

  val gcpDependencies = Seq(
    pubsub,
    hadoopGcp,
    googleCloudStorage,
    opentelemetry % Runtime
  ) ++ commonRuntimeDependencies

  val commonExclusions = Seq(
    ExclusionRule(organization = "org.apache.zookeeper", name     = "zookeeper"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-client"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-server"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-http"),
    ExclusionRule(organization = "org.eclipse.jetty", name        = "jetty-webapp"),
    ExclusionRule(organization = "org.apache.kerby"),
    ExclusionRule(organization = "org.apache.hadoop", name        = "hadoop-yarn-server-applicationhistoryservice"),
    ExclusionRule(organization = "org.apache.hadoop", name        = "hadoop-yarn-server-common"),
    ExclusionRule(organization = "com.github.joshelser", name     = "dropwizard-metrics-hadoop-metrics2-reporter"),
    ExclusionRule(organization = "org.apache.logging.log4j", name = "log4j-slf4j2-impl"),
    ExclusionRule(organization = "org.lz4", name                  = "lz4-java") // replaced by at.yawk.lz4:lz4-java
  )

}
