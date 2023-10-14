/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes

import cats.Functor
import cats.implicits._
import cats.effect.{Async, Resource, Sync}
import cats.effect.unsafe.implicits.global
import org.http4s.client.Client
import org.http4s.blaze.client.BlazeClientBuilder
import io.sentry.Sentry

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, SourceAndAck}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.lakes.processing.LakeWriter
import com.snowplowanalytics.snowplow.runtime.{AppInfo, HealthProbe}

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  resolver: Resolver[F],
  httpClient: Client[F],
  lakeWriter: LakeWriter[F],
  metrics: Metrics[F],
  cpuParallelism: Int, // use the runtime api to pick something sensible
  inMemBatchBytes: Long,
  windowing: EventProcessingConfig.TimedWindows
)

object Environment {

  def fromConfig[F[_]: Async, SourceConfig, SinkConfig](
    config: Config.WithIglu[SourceConfig, SinkConfig],
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toSink: SinkConfig => Resource[F, Sink[F]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- enableSentry[F](appInfo, config.main.monitoring.sentry)
      resolver <- mkResolver[F](config.iglu)
      httpClient <- BlazeClientBuilder[F].withExecutionContext(global.compute).resource
      badSink <- toSink(config.main.output.bad)
      windowing <- Resource.eval(EventProcessingConfig.TimedWindows.build(config.main.windowing))
      lakeWriter <- LakeWriter.build[F](config.main.spark, config.main.output.good)
      sourceAndAck <- Resource.eval(toSource(config.main.input))
      metrics <- Resource.eval(Metrics.build(config.main.monitoring.metrics))
      _ <- HealthProbe.resource(config.main.monitoring.healthProbe.port, isHealthy(config.main.monitoring.healthProbe, sourceAndAck))
    } yield Environment(
      appInfo         = appInfo,
      source          = sourceAndAck,
      badSink         = badSink,
      resolver        = resolver,
      httpClient      = httpClient,
      lakeWriter      = lakeWriter,
      metrics         = metrics,
      cpuParallelism  = chooseCpuParallelism(config.main),
      inMemBatchBytes = config.main.inMemBatchBytes,
      windowing       = windowing
    )

  private def enableSentry[F[_]: Sync](appInfo: AppInfo, config: Option[Config.Sentry]): Resource[F, Unit] =
    config match {
      case Some(c) =>
        val acquire = Sync[F].delay {
          Sentry.init { options =>
            options.setDsn(c.dsn)
            options.setRelease(appInfo.version)
            c.tags.foreach { case (k, v) =>
              options.setTag(k, v)
            }
          }
        }

        Resource.makeCase(acquire) {
          case (_, Resource.ExitCase.Errored(e)) => Sync[F].delay(Sentry.captureException(e)).void
          case _                                 => Sync[F].unit
        }
      case None =>
        Resource.unit[F]
    }

  private def mkResolver[F[_]: Async](resolverConfig: Resolver.ResolverConfig): Resource[F, Resolver[F]] =
    Resource.eval {
      Resolver
        .fromConfig[F](resolverConfig)
        .leftMap(e => new RuntimeException(s"Error while parsing Iglu resolver config", e))
        .value
        .rethrow
    }

  /*
  private def chooseInMemMaxBytes(config: AnyConfig): Long =
    (Runtime.getRuntime.maxMemory * config.inMemHeapFraction).toLong
   */

  private def chooseCpuParallelism(config: AnyConfig): Int =
    (Runtime.getRuntime.availableProcessors * config.cpuParallelismFraction)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

  private def isHealthy[F[_]: Functor](config: Config.HealthProbe, source: SourceAndAck[F]): F[HealthProbe.Status] =
    source.processingLatency.map { latency =>
      if (latency > config.unhealthyLatency)
        HealthProbe.Unhealthy(show"Processing latency is $latency")
      else
        HealthProbe.Healthy
    }
}
