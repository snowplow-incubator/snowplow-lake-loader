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
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, Metrics => CommonMetrics, Telemetry}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs.schemaCriterionDecoder
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._

case class Config[+Source, +Sink](
  input: Source,
  output: Config.Output[Sink],
  inMemBatchBytes: Long,
  cpuParallelismFraction: BigDecimal,
  numEagerWindows: Int,
  windowing: FiniteDuration,
  spark: Config.Spark,
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring,
  license: AcceptedLicense,
  skipSchemas: List[SchemaCriterion]
)

object Config {

  case class WithIglu[+Source, +Sink](main: Config[Source, Sink], iglu: ResolverConfig)

  case class Output[+Sink](good: Target, bad: Sink)

  sealed trait Target

  case class Delta(
    location: URI,
    dataSkippingColumns: List[String]
  ) extends Target

  case class Hudi(
    location: URI,
    hudiWriteOptions: Map[String, String],
    hudiTableOptions: Map[String, String]
  ) extends Target

  case class Iceberg(
    database: String,
    table: String,
    catalog: IcebergCatalog,
    location: URI
  ) extends Target

  sealed trait IcebergCatalog

  object IcebergCatalog {

    case class Hadoop(options: Map[String, String]) extends IcebergCatalog

    case class BigLake(
      project: String,
      name: String,
      region: String,
      options: Map[String, String]
    ) extends IcebergCatalog

    case class Glue(
      options: Map[String, String]
    ) extends IcebergCatalog
  }

  case class GcpUserAgent(productName: String)

  case class Spark(
    taskRetries: Int,
    conf: Map[String, String],
    gcpUserAgent: GcpUserAgent,
    writerParallelismFraction: BigDecimal
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
    implicit val configuration  = Configuration.default.withDiscriminator("type")
    implicit val icebergCatalog = deriveConfiguredDecoder[IcebergCatalog]
    implicit val target         = deriveConfiguredDecoder[Target]
    implicit val output         = deriveConfiguredDecoder[Output[Sink]]
    implicit val gcpUserAgent   = deriveConfiguredDecoder[GcpUserAgent]
    implicit val spark          = deriveConfiguredDecoder[Spark]
    implicit val sentryDecoder = deriveConfiguredDecoder[SentryM[Option]]
      .map[Option[Sentry]] {
        case SentryM(Some(dsn), tags) =>
          Some(SentryM[Id](dsn, tags))
        case SentryM(None, _) =>
          None
      }
    implicit val metricsDecoder     = deriveConfiguredDecoder[Metrics]
    implicit val healthProbeDecoder = deriveConfiguredDecoder[HealthProbe]
    implicit val monitoringDecoder  = deriveConfiguredDecoder[Monitoring]

    // TODO add specific lake-loader docs for license
    implicit val licenseDecoder =
      AcceptedLicense.decoder(AcceptedLicense.DocumentationLink("https://docs.snowplow.io/limited-use-license-1.0/"))

    deriveConfiguredDecoder[Config[Source, Sink]]
  }

}
