/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.sources

import cats.effect.Sync
import cats.effect.std.Random
import cats.implicits._

import scala.concurrent.duration.FiniteDuration

/**
 * Configures how events are fed into the [[EventProcessor]]
 *
 * @note
 *   This class is not for user-facing configuration. The application itself should have an opinion
 *   on these parameters, e.g. Enrich always wants NoWindowing, but Transformer always wants
 *   Windowing
 *
 * @param windowing
 *   Whether to open a new [[EventProcessor]] to handle a timed window of events (e.g. for the
 *   transformer) or whether to feed events to a single [[EventProcessor]] in a continuous endless
 *   stream (e.g. Enrich)
 */
case class EventProcessingConfig(windowing: EventProcessingConfig.Windowing)

object EventProcessingConfig {

  sealed trait Windowing
  case object NoWindowing extends Windowing

  /**
   * Configures windows e.g. for Transformer
   *
   * @param duration
   *   The base level duration between windows
   * @param firstWindowScaling
   *   A random factor to adjust the size of the first window. This addresses the situation where
   *   several parallel instances of the app all start at the same time. All instances in the group
   *   should end windows at slightly different times, so that downstream gets a more steady flow of
   *   completed batches.
   */
  case class TimedWindows(duration: FiniteDuration, firstWindowScaling: Double) extends Windowing

  object TimedWindows {
    def build[F[_]: Sync](duration: FiniteDuration): F[TimedWindows] =
      for {
        random <- Random.scalaUtilRandom
        factor <- random.nextDouble
      } yield TimedWindows(duration, factor)
  }

}
