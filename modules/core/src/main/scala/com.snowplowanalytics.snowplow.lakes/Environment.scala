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

import cats.{Functor, Monad}
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
      (lakeWriter, lakeWriterHealth) <- LakeWriter.build[F](config.main.spark, config.main.output.good)
      sourceAndAck <- Resource.eval(toSource(config.main.input))
      metrics <- Resource.eval(Metrics.build(config.main.monitoring.metrics))
      isHealthy = combineIsHealthy(sourceIsHealthy(config.main.monitoring.healthProbe, sourceAndAck), lakeWriterHealth)
      _ <- HealthProbe.resource(config.main.monitoring.healthProbe.port, isHealthy)
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

  private def sourceIsHealthy[F[_]: Functor](config: Config.HealthProbe, source: SourceAndAck[F]): F[HealthProbe.Status] =
    source.isHealthy(config.unhealthyLatency).map {
      case SourceAndAck.Healthy              => HealthProbe.Healthy
      case unhealthy: SourceAndAck.Unhealthy => HealthProbe.Unhealthy(unhealthy.show)
    }

  // TODO: This should move to common-streams
  private def combineIsHealthy[F[_]: Monad](status: F[HealthProbe.Status]*): F[HealthProbe.Status] =
    status.toList.foldM[F, HealthProbe.Status](HealthProbe.Healthy) {
      case (unhealthy: HealthProbe.Unhealthy, _) => Monad[F].pure(unhealthy)
      case (HealthProbe.Healthy, other)          => other
    }
}
