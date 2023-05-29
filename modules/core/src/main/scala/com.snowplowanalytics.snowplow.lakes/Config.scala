package com.snowplowanalytics.snowplow.lakes

import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._

import java.net.URI
import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig

case class Config[+Source, +Sink](
  input: Source,
  output: Config.Output[Sink],
  inMemHeapFraction: BigDecimal,
  windows: FiniteDuration,
  spark: Config.Spark
)

object Config {

  case class WithIglu[+Source, +Sink](main: Config[Source, Sink], iglu: ResolverConfig)

  case class Output[+Sink](good: Target, bad: Sink)

  sealed trait Target {
    def location: URI
  }

  case class Delta(location: URI) extends Target
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

  case class Spark(
    localDir: String,
    retries: Int,
    targetParquetSizeMB: Int,
    threads: Int,
    conf: Map[String, String]
  )

  implicit def decoder[Source: Decoder, Sink: Decoder]: Decoder[Config[Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val target = deriveConfiguredDecoder[Target]
    implicit val output = deriveConfiguredDecoder[Output[Sink]]
    implicit val spark = deriveConfiguredDecoder[Spark]
    deriveConfiguredDecoder[Config[Source, Sink]]
  }

}
