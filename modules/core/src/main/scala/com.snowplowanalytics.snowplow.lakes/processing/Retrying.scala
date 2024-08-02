/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.processing

import cats.Applicative
import cats.effect.Sync
import cats.implicits._

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import retry._
import retry.implicits.retrySyntaxError

import com.snowplowanalytics.snowplow.lakes.{Alert, AppHealth, Config, DestinationSetupErrorCheck, Monitoring}

object Retrying {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def withRetries[F[_]: Sync: Sleep, A](
    appHealth: AppHealth[F],
    config: Config.Retries,
    monitoring: Monitoring[F],
    toAlert: List[String] => Alert,
    destinationSetupErrorCheck: DestinationSetupErrorCheck
  )(
    action: F[A]
  ): F[A] =
    retryUntilSuccessful(appHealth, config, monitoring, toAlert, destinationSetupErrorCheck, action) <*
      appHealth.setServiceHealth(AppHealth.Service.SparkWriter, isHealthy = true)

  private def retryUntilSuccessful[F[_]: Sync: Sleep, A](
    appHealth: AppHealth[F],
    config: Config.Retries,
    monitoring: Monitoring[F],
    toAlert: List[String] => Alert,
    destinationSetupErrorCheck: DestinationSetupErrorCheck,
    action: F[A]
  ): F[A] =
    action
      .onError(_ => appHealth.setServiceHealth(AppHealth.Service.SparkWriter, isHealthy = false))
      .retryingOnSomeErrors(
        isWorthRetrying = checkingNestedExceptions(destinationSetupErrorCheck, _).nonEmpty.pure[F],
        policy          = policyForSetupErrors[F](config),
        onError         = logErrorAndSendAlert[F](monitoring, destinationSetupErrorCheck, toAlert, _, _)
      )
      .retryingOnAllErrors(
        policy  = policyForTransientErrors[F](config),
        onError = logError[F](_, _)
      )

  private def policyForSetupErrors[F[_]: Applicative](config: Config.Retries): RetryPolicy[F] =
    RetryPolicies.exponentialBackoff[F](config.setupErrors.delay)

  private def policyForTransientErrors[F[_]: Applicative](config: Config.Retries): RetryPolicy[F] =
    RetryPolicies.fullJitter[F](config.transientErrors.delay).join(RetryPolicies.limitRetries(config.transientErrors.attempts - 1))

  private def logErrorAndSendAlert[F[_]: Sync](
    monitoring: Monitoring[F],
    destinationSetupErrorCheck: DestinationSetupErrorCheck,
    toAlert: List[String] => Alert,
    error: Throwable,
    details: RetryDetails
  ): F[Unit] =
    logError(error, details) *> monitoring.alert(toAlert(checkingNestedExceptions(destinationSetupErrorCheck, error)))

  private def logError[F[_]: Sync](error: Throwable, details: RetryDetails): F[Unit] =
    Logger[F].error(error)(s"Executing command failed. ${extractRetryDetails(details)}")

  private def extractRetryDetails(details: RetryDetails): String = details match {
    case RetryDetails.GivingUp(totalRetries, totalDelay) =>
      s"Giving up on retrying, total retries: $totalRetries, total delay: ${totalDelay.toSeconds} seconds"
    case RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, cumulativeDelay) =>
      s"Will retry in ${nextDelay.toMillis} milliseconds, retries so far: $retriesSoFar, total delay so far: ${cumulativeDelay.toMillis} milliseconds"
  }

  // Returns a list of reasons of why this was a destination setup error.
  // Or empty list if this was not caused by a destination setup error
  private def checkingNestedExceptions(
    destinationSetupErrorCheck: DestinationSetupErrorCheck,
    t: Throwable
  ): List[String] =
    (destinationSetupErrorCheck(t), Option(t.getCause)) match {
      case (Some(msg), Some(cause)) => msg :: checkingNestedExceptions(destinationSetupErrorCheck, cause)
      case (Some(msg), None)        => List(msg)
      case (None, Some(cause))      => checkingNestedExceptions(destinationSetupErrorCheck, cause)
      case (None, None)             => Nil
    }
}
