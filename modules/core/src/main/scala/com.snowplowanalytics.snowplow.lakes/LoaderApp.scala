package com.snowplowanalytics.snowplow.lakes

import cats.effect.{ExitCode, IO, Resource}
import io.circe.Decoder
import com.monovore.decline.effect.CommandIOApp
import com.monovore.decline.Opts

import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}

abstract class LoaderApp[SourceConfig: Decoder, SinkConfig: Decoder](
  name: String,
  dockerAlias: String,
  version: String
) extends CommandIOApp(name = LoaderApp.helpCommand(dockerAlias), header = dockerAlias, version = version) {

  type SinkProvider = SinkConfig => Resource[IO, Sink[IO]]
  type SourceProvider = SourceConfig => SourceAndAck[IO]

  def source: SourceProvider
  def badSink: SinkProvider

  final def main: Opts[IO[ExitCode]] = Run.fromCli(BadRowProcessor(name, version), source, badSink)

}

object LoaderApp {

  private def helpCommand(dockerAlias: String) = s"docker run $dockerAlias"
}
