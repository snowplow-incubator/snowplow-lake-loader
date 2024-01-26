/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.sinks.internal

import cats.effect.Async
import org.typelevel.log4cats.Logger
import retry._

import com.snowplowanalytics.snowplow.sinks.{Sink, Sinkable}

import scala.concurrent.duration.FiniteDuration

/** This might be not-needed.  Most sinks already have retries built in to the underlying client */
object Retries {

  def sink[F[_]: Async: Logger](maxRetries: Int, baseDelay: FiniteDuration)(f: List[Sinkable] => F[Unit]): Sink[F] = {

    def onError(t: Throwable, details: RetryDetails): F[Unit] = {
      val _ = t // The error can be logged by the surrounding application, not by this lib
      if (details.givingUp)
        Logger[F].warn(s"Error writing batch of events to the output sink. GIVING UP for this batch of events.")
      else
        Logger[F].warn(s"Error writing batch of events to the output sink. RETRYING this batch of events....")
    }

    val policy = RetryPolicies.fullJitter[F](baseDelay).join(RetryPolicies.limitRetries[F](maxRetries))

    Sink[F] { batch =>
      retryingOnAllErrors(policy, onError)(f(batch))
    }
  }

}
