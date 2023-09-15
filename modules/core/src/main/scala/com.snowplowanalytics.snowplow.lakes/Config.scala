/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes

import cats.Id
import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._

import java.net.URI
import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.snowplow.loaders.{Metrics => CommonMetrics, Telemetry}

case class Config[+Source, +Sink](
  input: Source,
  output: Config.Output[Sink],
  inMemBatchBytes: Long,
  cpuParallelismFraction: BigDecimal,
  windowing: FiniteDuration,
  spark: Config.Spark,
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring
)

object Config {

  case class WithIglu[+Source, +Sink](main: Config[Source, Sink], iglu: ResolverConfig)

  case class Output[+Sink](good: Target, bad: Sink)

  sealed trait Target {
    def location: URI
  }

  case class Delta(
    location: URI,
    dataSkippingColumns: List[String]
  ) extends Target

  sealed trait Iceberg extends Target

  case class IcebergSnowflake(
    host: String,
    user: String,
    role: Option[String],
    password: String,
    database: String,
    schema: String,
    table: String,
    location: URI
  ) extends Iceberg
      with Target

  case class IcebergBigLake(
    project: String,
    catalog: String,
    database: String,
    table: String,
    connection: String,
    bqDataset: String,
    region: String, // (of biglake) also known as location in biglake docs.
    location: URI
  ) extends Iceberg
      with Target

  case class Spark(
    taskRetries: Int,
    conf: Map[String, String]
  )

  case class Metrics(
    statsd: Option[CommonMetrics.StatsdConfig]
  )

  private case class StatsdUnresolved(
    hostname: Option[String],
    port: Int,
    tags: Map[String, String],
    period: FiniteDuration,
    prefix: String
  )

  private object Statsd {

    def resolve(statsd: StatsdUnresolved): Option[CommonMetrics.StatsdConfig] =
      statsd match {
        case StatsdUnresolved(Some(hostname), port, tags, period, prefix) =>
          Some(CommonMetrics.StatsdConfig(hostname, port, tags, period, prefix))
        case StatsdUnresolved(None, _, _, _, _) =>
          None
      }
  }

  case class SentryM[M[_]](
    dsn: M[String],
    tags: Map[String, String]
  )

  type Sentry = SentryM[Id]

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry]
  )

  implicit def decoder[Source: Decoder, Sink: Decoder]: Decoder[Config[Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val target = deriveConfiguredDecoder[Target]
    implicit val output = deriveConfiguredDecoder[Output[Sink]]
    implicit val spark = deriveConfiguredDecoder[Spark]
    implicit val telemetry = deriveConfiguredDecoder[Telemetry.Config]
    implicit val statsdDecoder = deriveConfiguredDecoder[StatsdUnresolved].map(Statsd.resolve(_))
    implicit val sentryDecoder = deriveConfiguredDecoder[SentryM[Option]]
      .map[Option[Sentry]] {
        case SentryM(Some(dsn), tags) =>
          Some(SentryM[Id](dsn, tags))
        case SentryM(None, _) =>
          None
      }
    implicit val metricsDecoder = deriveConfiguredDecoder[Metrics]
    implicit val monitoringDecoder = deriveConfiguredDecoder[Monitoring]
    deriveConfiguredDecoder[Config[Source, Sink]]
  }

}
