/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import cats.effect.implicits._
import cats.implicits._
import cats.effect.kernel.Unique
import cats.effect.{Async, Deferred, Ref, Sync}
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.pubsub.v1.AcknowledgeRequest
import com.google.api.gax.grpc.GrpcCallContext
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration

import com.snowplowanalytics.snowplow.sources.internal.Checkpointer
import com.snowplowanalytics.snowplow.pubsub.FutureInterop
import com.snowplowanalytics.snowplow.sources.pubsub.v2.PubsubRetryOps.implicits._

/**
 * The Pubsub checkpointer
 *
 * @param subscription
 *   Pubsub subscription name
 * @param deferredResources
 *   Resources needed so we can ack/nack messages. This is wrapped in `Deferred` because the
 *   resources are not available until the application calls `.stream` on the `LowLevelSource`. This
 *   is a limitation in the design of the common-streams Source interface.
 */
class PubsubCheckpointer[F[_]: Async](
  subscription: PubsubSourceConfigV2.Subscription,
  deferredResources: Deferred[F, PubsubCheckpointer.Resources[F]]
) extends Checkpointer[F, Vector[Unique.Token]] {

  import PubsubCheckpointer._

  private implicit def logger: Logger[F] = Slf4jLogger.getLogger[F]

  override def combine(x: Vector[Unique.Token], y: Vector[Unique.Token]): Vector[Unique.Token] =
    x |+| y

  override val empty: Vector[Unique.Token] = Vector.empty

  /**
   * Ack some batches of messages received from pubsub
   *
   * @param c
   *   tokens which are keys to batch data held in the shared state
   */
  override def ack(c: Vector[Unique.Token]): F[Unit] =
    for {
      Resources(stub, refAckIds) <- deferredResources.get
      ackDatas <- refAckIds.modify(m => (m.removedAll(c), c.flatMap(m.get)))
      grouped = ackDatas.groupBy(_.channelAffinity)
      _ <- grouped.toVector.parTraverse_ { case (channelAffinity, ackDatas) =>
             ackDatas.flatMap(_.ackIds).grouped(1000).toVector.traverse_ { ackIds =>
               val request = AcknowledgeRequest.newBuilder.setSubscription(subscription.show).addAllAckIds(ackIds.asJava).build
               val context = GrpcCallContext.createDefault.withChannelAffinity(channelAffinity)
               val attempt = for {
                 apiFuture <- Sync[F].delay(stub.acknowledgeCallable.futureCall(request, context))
                 _ <- FutureInterop.fromFuture[F, com.google.protobuf.Empty](apiFuture)
               } yield ()
               attempt.retryingOnTransientGrpcFailures
                 .recoveringOnGrpcInvalidArgument { s =>
                   // This can happen if ack IDs have expired before we acked
                   Logger[F].info(s"Ignoring error from GRPC when acking: ${s.getDescription}")
                 }
             }
           }
    } yield ()

  /**
   * Nack some batches of messages received from pubsub
   *
   * @param c
   *   tokens which are keys to batch data held in the shared state
   */
  override def nack(c: Vector[Unique.Token]): F[Unit] =
    for {
      Resources(stub, refAckIds) <- deferredResources.get
      ackDatas <- refAckIds.modify(m => (m.removedAll(c), c.flatMap(m.get)))
      grouped = ackDatas.groupBy(_.channelAffinity)
      _ <- grouped.toVector.parTraverse_ { case (channelAffinity, ackDatas) =>
             val ackIds = ackDatas.flatMap(_.ackIds)
             // A nack is just a modack with zero duration
             Utils.modAck[F](subscription, stub, ackIds, Duration.Zero, channelAffinity)
           }
    } yield ()
}

private object PubsubCheckpointer {

  /**
   * Resources needed by `PubsubCheckpointer` so it can ack/nack messages
   *
   * @param stub
   *   The GRPC stub needed to execute the ack/nack RPCs
   * @param refState
   *   A map from tokens to the data held about a batch of messages received from pubsub. The map is
   *   wrapped in a `Ref` because it is concurrently modified by the source adding new batches to
   *   the state.
   */
  case class Resources[F[_]](stub: SubscriberStub, refState: Ref[F, Map[Unique.Token, PubsubBatchState]])

}
