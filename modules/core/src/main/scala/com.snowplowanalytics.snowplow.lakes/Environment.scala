package com.snowplowanalytics.snowplow.lakes

import cats.implicits._
import cats.effect.{Async, Resource}
import cats.effect.unsafe.implicits.global
import org.http4s.client.Client
import org.http4s.blaze.client.BlazeClientBuilder

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, SourceAndAck}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.lakes.processing.LakeWriter

case class Environment[F[_]](
  processor: BadRowProcessor,
  source: SourceAndAck[F],
  badSink: Sink[F],
  resolver: Resolver[F],
  httpClient: Client[F],
  lakeWriter: LakeWriter[F],
  inMemMaxBytes: Long, // use the runtime api to pick something sensible
  windowing: EventProcessingConfig.TimedWindows
)

object Environment {

  def fromConfig[F[_]: Async, SourceConfig, SinkConfig](
    config: Config.WithIglu[SourceConfig, SinkConfig],
    processor: BadRowProcessor,
    source: SourceConfig => SourceAndAck[F],
    sink: SinkConfig => Resource[F, Sink[F]]
  ): Resource[F, Environment[F]] =
    for {
      resolver <- mkResolver[F](config.iglu)
      httpClient <- BlazeClientBuilder[F].withExecutionContext(global.compute).resource
      badSink <- sink(config.main.output.bad)
      windowing <- Resource.eval(EventProcessingConfig.TimedWindows.build(config.main.windows))
      lakeWriter <- LakeWriter.build[F](config.main.spark, config.main.output.good)
    } yield Environment(
      processor = processor,
      source = source(config.main.input),
      badSink = badSink,
      resolver = resolver,
      httpClient = httpClient,
      lakeWriter = lakeWriter,
      inMemMaxBytes = chooseInMemMaxBytes(config.main),
      windowing = windowing
    )

  private def mkResolver[F[_]: Async](resolverConfig: Resolver.ResolverConfig): Resource[F, Resolver[F]] =
    Resource.eval {
      Resolver
        .fromConfig[F](resolverConfig)
        .leftMap(e => new RuntimeException(s"Error while parsing Iglu resolver config", e))
        .value
        .rethrow
    }

  private def chooseInMemMaxBytes(config: AnyConfig): Long =
    (Runtime.getRuntime.maxMemory * config.inMemHeapFraction).toLong

}
