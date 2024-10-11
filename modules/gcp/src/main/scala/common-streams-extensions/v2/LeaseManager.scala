/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.kernel.Unique
import cats.implicits._
import cats.effect.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.google.cloud.pubsub.v1.stub.SubscriberStub

import scala.concurrent.duration.Duration

private trait LeaseManager[F[_], A] {
  def manageLeases(in: A): F[Unique.Token]
  def stopManagingLeases(tokens: Vector[Unique.Token]): F[Unit]
}

private object LeaseManager {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def resource[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    ref: Ref[F, Map[Unique.Token, PubsubBatchState]],
    channelAffinity: Int
  ): Resource[F, LeaseManager[F, SubscriberAction.ProcessRecords]] =
    extendDeadlinesInBackground[F](config, stub, ref, channelAffinity)
      .as(impl(config, stub, ref, channelAffinity))

  private def impl[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    ref: Ref[F, Map[Unique.Token, PubsubBatchState]],
    channelAffinity: Int
  ): LeaseManager[F, SubscriberAction.ProcessRecords] = new LeaseManager[F, SubscriberAction.ProcessRecords] {

    def manageLeases(in: SubscriberAction.ProcessRecords): F[Unique.Token] =
      Sync[F].uncancelable { _ =>
        Unique[F].unique.flatMap { token =>
          val deadline = in.timeReceived.plusMillis(config.durationPerAckExtension.toMillis)
          val ackIds   = in.records.map(_.getAckId)
          val state    = PubsubBatchState(deadline, ackIds, channelAffinity)
          ref.update(_ + (token -> state)).as(token)
        }
      }

    def stopManagingLeases(tokens: Vector[Unique.Token]): F[Unit] =
      Sync[F].uncancelable { _ =>
        for {
          ackIds <- ref.modify(m => (m.removedAll(tokens), tokens.flatMap(m.get).flatMap(_.ackIds.toVector)))
          _ <- Utils.modAck[F](config.subscription, stub, ackIds, Duration.Zero, channelAffinity)
        } yield ()
      }
  }

  private def extendDeadlinesInBackground[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]],
    channelAffinity: Int
  ): Resource[F, Unit] = {
    def go: F[Unit] = for {
      now <- Sync[F].realTimeInstant
      minAllowedDeadline = now.plusMillis((config.minRemainingDeadline * config.durationPerAckExtension.toMillis).toLong)
      newDeadline        = now.plusMillis(config.durationPerAckExtension.toMillis)
      toExtend <- refStates.modify { m =>
                    val toExtend = m.filter { case (_, batchState) =>
                      batchState.channelAffinity === channelAffinity && batchState.currentDeadline.isBefore(minAllowedDeadline)
                    }
                    val fixed = toExtend.view
                      .mapValues(_.copy(currentDeadline = newDeadline))
                      .toMap
                    (m ++ fixed, toExtend.values.toVector)
                  }
      _ <- if (toExtend.isEmpty)
             Sync[F].sleep(0.5 * config.minRemainingDeadline * config.durationPerAckExtension)
           else {
             val ackIds = toExtend.sortBy(_.currentDeadline).flatMap(_.ackIds.toVector)
             Utils.modAck[F](config.subscription, stub, ackIds, config.durationPerAckExtension, channelAffinity)
           }
      _ <- go
    } yield ()
    go.background.void
  }

}
