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
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}

import java.nio.file.Path

object Run {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def fromCli[F[_]: Async, SourceConfig: Decoder, SinkConfig: Decoder](
    processor: BadRowProcessor,
    toSource: SourceConfig => SourceAndAck[F],
    toBadSink: SinkConfig => Resource[F, Sink[F]]
  ): Opts[F[ExitCode]] = {
    val configPathOpt = Opts.option[Path]("config", help = "path to config file")
    val igluPathOpt = Opts.option[Path]("iglu-config", help = "path to iglu resolver config file")
    (configPathOpt, igluPathOpt).mapN(fromConfigPaths(processor, toSource, toBadSink, _, _))
  }

  private def fromConfigPaths[F[_]: Async, SourceConfig: Decoder, SinkConfig: Decoder](
    processor: BadRowProcessor,
    toSource: SourceConfig => SourceAndAck[F],
    toBadSink: SinkConfig => Resource[F, Sink[F]],
    pathToConfig: Path,
    pathToResolver: Path
  ): F[ExitCode] = {

    val eitherT = for {
      config <- ConfigParser.fromPaths[F, SourceConfig, SinkConfig](pathToConfig, pathToResolver)
      _ <- EitherT.right[ExitCode](fromConfig(processor, toSource, toBadSink, config))
    } yield ExitCode.Success

    eitherT.merge.handleErrorWith { e =>
      Logger[F].error(e)("Exiting") >>
        prettyLogException(e).as(ExitCode.Error)
    }
  }

  private def fromConfig[F[_]: Async, SourceConfig, SinkConfig](
    processor: BadRowProcessor,
    toSource: SourceConfig => SourceAndAck[F],
    toBadSink: SinkConfig => Resource[F, Sink[F]],
    config: Config.WithIglu[SourceConfig, SinkConfig]
  ): F[ExitCode] =
    Environment.fromConfig(config, processor, toSource, toBadSink).use { env =>
      Processing.stream(config.main, env).compile.drain.as(ExitCode.Success)
    }

  def prettyLogException[F[_]: Sync](e: Throwable): F[Unit] = {

    def logCause(e: Throwable): F[Unit] =
      Option(e.getCause) match {
        case Some(e) => Logger[F].error(s"caused by: ${e.getMessage}") >> logCause(e)
        case None => Sync[F].unit
      }

    Logger[F].error(e.getMessage) >> logCause(e)
  }
}
