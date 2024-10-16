/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import cats.implicits._
import cats.effect.Async
import com.google.api.gax.rpc.{ApiException, StatusCode}
import io.grpc.Status
import org.typelevel.log4cats.Logger
import retry.RetryPolicies
import retry.implicits._

import scala.concurrent.duration.DurationDouble

private object PubsubRetryOps {

  object implicits {
    implicit class Ops[F[_], A](val f: F[A]) extends AnyVal {

      def retryingOnTransientGrpcFailures(implicit F: Async[F], L: Logger[F]): F[A] =
        f.retryingOnSomeErrors(
          isWorthRetrying = { e => isRetryableException(e).pure[F] },
          policy          = RetryPolicies.fullJitter(1.second),
          onError = { case (t, _) =>
            Logger[F].info(t)(s"Pubsub retryable GRPC error will be retried: ${t.getMessage}")
          }
        )

      def recoveringOnGrpcInvalidArgument(f2: Status => F[A])(implicit F: Async[F]): F[A] =
        f.recoverWith {
          case StatusFromThrowable(s) if s.getCode.equals(Status.Code.INVALID_ARGUMENT) =>
            f2(s)
        }
    }
  }

  private object StatusFromThrowable {
    def unapply(t: Throwable): Option[Status] =
      Some(Status.fromThrowable(t))
  }

  def isRetryableException: Throwable => Boolean = {
    case apiException: ApiException =>
      apiException.getStatusCode.getCode match {
        case StatusCode.Code.DEADLINE_EXCEEDED  => true
        case StatusCode.Code.INTERNAL           => true
        case StatusCode.Code.CANCELLED          => true
        case StatusCode.Code.RESOURCE_EXHAUSTED => true
        case StatusCode.Code.ABORTED            => true
        case StatusCode.Code.UNKNOWN            => true
        case StatusCode.Code.UNAVAILABLE        => !apiException.getMessage().contains("Server shutdownNow invoked")
        case _                                  => false
      }
    case _ =>
      false
  }
}
