/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import cats.Id
import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._
import com.comcast.ip4s.Port

import java.net.URI
import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics, Telemetry}
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._

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

  case class SentryM[M[_]](
    dsn: M[String],
    tags: Map[String, String]
  )

  type Sentry = SentryM[Id]

  case class HealthProbe(port: Port, unhealthyLatency: FiniteDuration)

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry],
    healthProbe: HealthProbe
  )

  implicit def decoder[Source: Decoder, Sink: Decoder]: Decoder[Config[Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val target        = deriveConfiguredDecoder[Target]
    implicit val output        = deriveConfiguredDecoder[Output[Sink]]
    implicit val spark         = deriveConfiguredDecoder[Spark]
    implicit val sentryDecoder = deriveConfiguredDecoder[SentryM[Option]]
      .map[Option[Sentry]] {
        case SentryM(Some(dsn), tags) =>
          Some(SentryM[Id](dsn, tags))
        case SentryM(None, _) =>
          None
      }
    implicit val metricsDecoder = deriveConfiguredDecoder[Metrics]
    implicit val healthProbeDecoder = deriveConfiguredDecoder[HealthProbe]
    implicit val monitoringDecoder  = deriveConfiguredDecoder[Monitoring]
    deriveConfiguredDecoder[Config[Source, Sink]]
  }

}
