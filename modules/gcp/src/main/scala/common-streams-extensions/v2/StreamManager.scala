/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import cats.data.NonEmptyVector
import cats.effect.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import cats.implicits._

// pubsub
import com.google.api.gax.grpc.GrpcCallContext
import com.google.api.gax.rpc.{ResponseObserver, StreamController}
import com.google.pubsub.v1.{StreamingPullRequest, StreamingPullResponse}
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import io.grpc.Status

import scala.jdk.CollectionConverters._

import java.util.concurrent.LinkedBlockingQueue
import java.util.UUID
import java.time.Instant

private trait StreamManager[F[_]] {
  def startAgain: F[Unit]
}

private object StreamManager {

  def resource[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    actionQueue: LinkedBlockingQueue[SubscriberAction],
    channelAffinity: Int
  ): Resource[F, StreamManager[F]] =
    for {
      clientId <- Resource.eval(Sync[F].delay(UUID.randomUUID))
      hotswap <- Hotswap.create[F, Unit]
    } yield new StreamManager[F] {

      def startAgain: F[Unit] =
        hotswap.swap(initializeStreamingPull(config, stub, actionQueue, channelAffinity, clientId))
    }

  private def initializeStreamingPull[F[_]: Sync](
    config: PubsubSourceConfigV2,
    subStub: SubscriberStub,
    actionQueue: LinkedBlockingQueue[SubscriberAction],
    channelAffinity: Int,
    clientId: UUID
  ): Resource[F, Unit] = {

    val observer = new ResponseObserver[StreamingPullResponse] {
      var controller: StreamController = _
      override def onResponse(response: StreamingPullResponse): Unit = {
        val messages = response.getReceivedMessagesList.asScala.toVector
        NonEmptyVector.fromVector(messages) match {
          case Some(nev) =>
            val action = SubscriberAction.ProcessRecords(nev, Instant.now())
            actionQueue.put(action)
          case None =>
            // messages was empty
            controller.request(1)
        }
      }

      override def onStart(c: StreamController): Unit = {
        controller = c
        controller.disableAutoInboundFlowControl()
        actionQueue.put(SubscriberAction.Ready(controller))
      }

      override def onError(t: Throwable): Unit =
        actionQueue.put(SubscriberAction.SubscriberError(t))

      override def onComplete(): Unit = ()

    }

    val context = GrpcCallContext.createDefault.withChannelAffinity(channelAffinity)

    val request = StreamingPullRequest.newBuilder
      .setSubscription(config.subscription.show)
      .setStreamAckDeadlineSeconds(config.durationPerAckExtension.toSeconds.toInt)
      .setClientId(clientId.toString)
      .setMaxOutstandingMessages(0)
      .setMaxOutstandingBytes(0)
      .build

    Resource
      .make(Sync[F].delay(subStub.streamingPullCallable.splitCall(observer, context))) { stream =>
        Sync[F].delay(stream.closeSendWithError(Status.CANCELLED.asException))
      }
      .evalMap { stream =>
        Sync[F].delay(stream.send(request))
      }
      .void

  }
}
