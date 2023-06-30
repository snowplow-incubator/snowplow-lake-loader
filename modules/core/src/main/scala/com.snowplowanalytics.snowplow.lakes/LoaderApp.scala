/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
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
