package com.snowplowanalytics.snowplow.sources

import scala.concurrent.duration.FiniteDuration

case class SourceConfig(windowing: SourceConfig.Windowing)

object SourceConfig {

  sealed trait Windowing
  case object NoWindowing extends Windowing
  case class TimedWindows(duration: FiniteDuration) extends Windowing

}
