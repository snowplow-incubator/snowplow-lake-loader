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

import cats.implicits._
import cats.effect.{Async, ExitCode, Resource, Sync}
import cats.data.EitherT
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.circe.Decoder
import com.monovore.decline.Opts

import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.lakes.processing.Processing
import com.snowplowanalytics.snowplow.runtime.{AppInfo, ConfigParser, LogUtils, Telemetry}

import java.nio.file.Path

object Run {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def fromCli[F[_]: Async, SourceConfig: Decoder, SinkConfig: Decoder](
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toBadSink: SinkConfig => Resource[F, Sink[F]]
  ): Opts[F[ExitCode]] = {
    val configPathOpt = Opts.option[Path]("config", help = "path to config file")
    val igluPathOpt   = Opts.option[Path]("iglu-config", help = "path to iglu resolver config file")
    (configPathOpt, igluPathOpt).mapN(fromConfigPaths(appInfo, toSource, toBadSink, _, _))
  }

  private def fromConfigPaths[F[_]: Async, SourceConfig: Decoder, SinkConfig: Decoder](
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toBadSink: SinkConfig => Resource[F, Sink[F]],
    pathToConfig: Path,
    pathToResolver: Path
  ): F[ExitCode] = {

    val eitherT = for {
      config <- ConfigParser.configFromFile[F, Config[SourceConfig, SinkConfig]](pathToConfig)
      resolver <- ConfigParser.igluResolverFromFile(pathToResolver)
      configWithIglu = Config.WithIglu(config, resolver)
      _ <- EitherT.right[String](fromConfig(appInfo, toSource, toBadSink, configWithIglu))
    } yield ExitCode.Success

    eitherT
      .leftSemiflatMap { s: String =>
        Logger[F].error(s).as(ExitCode.Error)
      }
      .merge
      .handleErrorWith { e =>
        Logger[F].error(e)("Exiting") >>
          LogUtils.prettyLogException(e).as(ExitCode.Error)
      }
  }

  private def fromConfig[F[_]: Async, SourceConfig, SinkConfig](
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toBadSink: SinkConfig => Resource[F, Sink[F]],
    config: Config.WithIglu[SourceConfig, SinkConfig]
  ): F[ExitCode] =
    Environment.fromConfig(config, appInfo, toSource, toBadSink).use { env =>
      Processing
        .stream(env)
        .concurrently(Telemetry.stream(config.main.telemetry, env.appInfo, env.httpClient))
        .concurrently(env.metrics.report)
        .compile
        .drain
        .as(ExitCode.Success)
    }

}
