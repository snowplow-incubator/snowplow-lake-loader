/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import cats.effect.{Async, Sync}
import cats.implicits._
import org.typelevel.log4cats.Logger

import com.google.api.gax.grpc.GrpcCallContext
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.ModifyAckDeadlineRequest
import com.snowplowanalytics.snowplow.pubsub.FutureInterop
import com.snowplowanalytics.snowplow.sources.pubsub.v2.PubsubRetryOps.implicits._

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

private object Utils {

  def modAck[F[_]: Async: Logger](
    subscription: PubsubSourceConfigV2.Subscription,
    stub: SubscriberStub,
    ackIds: Vector[String],
    duration: FiniteDuration,
    channelAffinity: Int
  ): F[Unit] =
    ackIds.grouped(1000).toVector.traverse_ { group =>
      val request = ModifyAckDeadlineRequest.newBuilder
        .setSubscription(subscription.show)
        .addAllAckIds(group.asJava)
        .setAckDeadlineSeconds(duration.toSeconds.toInt)
        .build
      val context = GrpcCallContext.createDefault.withChannelAffinity(channelAffinity)
      val io = for {
        apiFuture <- Sync[F].delay(stub.modifyAckDeadlineCallable.futureCall(request, context))
        _ <- FutureInterop.fromFuture(apiFuture)
      } yield ()

      io.retryingOnTransientGrpcFailures
        .recoveringOnGrpcInvalidArgument { s =>
          // This can happen if ack IDs were acked before we modAcked
          Logger[F].info(s"Ignoring error from GRPC when modifying ack IDs: ${s.getDescription}")
        }
    }

}
