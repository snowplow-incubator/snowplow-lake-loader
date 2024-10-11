/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import cats.effect.{Async, Deferred, Resource, Sync}
import cats.effect.kernel.{DeferredSink, Unique}
import cats.effect.std.AtomicCell
import cats.implicits._
import cats.effect.implicits._
import com.google.api.gax.rpc.StreamController
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}

private class SourceCoordinator[F[_]: Async, A] private (
  config: PubsubSourceConfigV2,
  status: AtomicCell[F, SourceCoordinator.Status[F, A]],
  streamManager: StreamManager[F],
  leaseManager: LeaseManager[F, A],
  channelAffinity: Int
) {
  import SourceCoordinator._

  private implicit val logger: Logger[F] = Slf4jLogger.getLogger[F]

  private def raiseForIllegalState[B](status: Status[F, A], thingCannotDo: String): F[B] =
    Sync[F].raiseError(
      new IllegalStateException(s"${getClass.getName} cannot $thingCannotDo when in state ${status.getClass.getSimpleName}")
    )

  private def jitter(base: FiniteDuration): F[FiniteDuration] =
    Sync[F].delay {
      (base.toMillis * (1.0 + 0.2 * scala.util.Random.nextDouble())).toLong.millis
    }

  private def initialBackoffWithJitter: F[FiniteDuration] =
    jitter(100.millis)

  def pull: F[TokenedA[A]] =
    status
      .evalModify[F[TokenedA[A]]] {
        case Status.Shutdown(since) =>
          for {
            deferred <- Deferred[F, TokenedA[A]]
            now <- Sync[F].realTime
            nextBackoff <- initialBackoffWithJitter
          } yield {
            val pause = if (now - since > nextBackoff) Sync[F].unit else Sync[F].sleep(now - since - nextBackoff)
            val get   = pause >> streamManager.startAgain >> deferred.get
            Status.Initializing[F, A](nextBackoff, BufferOrAwaiter.Awaiter(deferred)) -> get
          }
        case Status.Initializing(lastBackoff, BufferOrAwaiter.Buffer(buffered)) =>
          // Downstream wants to pull some message, and by luck we are already initializing a streaming pull
          buffered match {
            case head +: tail =>
              Sync[F].pure(Status.Initializing(lastBackoff, BufferOrAwaiter.Buffer[F, A](tail)) -> head.pure[F])
            case _ =>
              for {
                deferred <- Deferred[F, TokenedA[A]]
              } yield Status.Initializing(lastBackoff, BufferOrAwaiter.Awaiter(deferred)) -> deferred.get
          }
        case status @ Status.Initializing(_, BufferOrAwaiter.Awaiter(_)) =>
          // Illegal state because we only start initializing when downstreams calls `pull`
          // ...so we only get here if downstream calls `pull` twice without waiting
          raiseForIllegalState(status, "pull")
        case status @ Status.Requesting(_, BufferOrAwaiter.Awaiter(_), _) =>
          // We only get here if downstream calls `pull` twice without waiting
          raiseForIllegalState(status, "pull")
        case Status.Requesting(controller, BufferOrAwaiter.Buffer(buffered), lastBackoff) =>
          // Downstream wants to pull some message, and by luck we are already requesting some
          buffered match {
            case head +: tail =>
              Sync[F].pure(Status.Requesting[F, A](controller, BufferOrAwaiter.Buffer(tail), lastBackoff) -> head.pure[F])
            case _ =>
              Deferred[F, TokenedA[A]].map { deferred =>
                Status.Requesting[F, A](controller, BufferOrAwaiter.Awaiter(deferred), lastBackoff) -> deferred.get
              }
          }
        case Status.AwaitingConsumer(controller, buffered, keepAliveDue) =>
          buffered match {
            case head +: tail if tail.size > config.prefetchMin =>
              Sync[F].pure(Status.AwaitingConsumer[F, A](controller, tail, keepAliveDue) -> head.pure[F])
            case head +: tail =>
              val todo = for {
                delay <- jitter(10.millis * tail.size.toLong)
                _ <- Sync[F].sleep(delay)
                _ <- Sync[F].delay(controller.request(1))
              } yield ()
              Sync[F].pure(Status.Requesting[F, A](controller, BufferOrAwaiter.Buffer(tail), None) -> todo.start.as(head))
            case _ =>
              for {
                deferred <- Deferred[F, TokenedA[A]]
                _ <- Sync[F].delay(controller.request(1))
              } yield Status.Requesting[F, A](controller, BufferOrAwaiter.Awaiter(deferred), None) -> deferred.get
          }
      }
      .flatten

  def handleStreamError: F[Unit] =
    status
      .evalModify[F[Unit]] {
        case Status.Requesting(_, receive, Some(lastBackoff)) =>
          val nextBackoff = (lastBackoff * 2).min(10.seconds)
          val todo        = Sync[F].sleep(nextBackoff) >> streamManager.startAgain
          Sync[F].pure(Status.Initializing[F, A](nextBackoff, receive) -> todo)
        case Status.Requesting(_, receive, None) =>
          initialBackoffWithJitter.map { nextBackoff =>
            val todo = Sync[F].sleep(nextBackoff) >> streamManager.startAgain
            Status.Initializing[F, A](nextBackoff, receive) -> todo
          }
        case shutdown: Status.Shutdown[F, A] =>
          Sync[F].pure(shutdown -> Sync[F].unit)
        case Status.AwaitingConsumer(_, buffered, _) =>
          initialBackoffWithJitter.map { nextBackoff =>
            val todo = Sync[F].sleep(nextBackoff) >> streamManager.startAgain
            Status.Initializing[F, A](nextBackoff, BufferOrAwaiter.Buffer(buffered)) -> todo
          }
        case status: Status.Initializing[F, A] =>
          raiseForIllegalState(status, "handle stream error")
      }
      .flatten

  def receiveItem(in: A): F[Unit] =
    status
      .evalModify[F[Unit]] {
        case Status.Requesting(controller, BufferOrAwaiter.Awaiter(deferred), _) =>
          if (config.prefetchMin > 0) {
            for {
              token <- leaseManager.manageLeases(in)
              _ <- deferred.complete(TokenedA(token, in))
              _ <- Sync[F].delay(controller.request(1))
            } yield Status.Requesting(controller, BufferOrAwaiter.Buffer(Vector.empty), None) -> Sync[F].unit
          } else {
            for {
              now <- Sync[F].realTime
              token <- leaseManager.manageLeases(in)
              _ <- deferred.complete(TokenedA(token, in))
              keepAliveDelay <- jitter(config.progressTimeout)
            } yield Status.AwaitingConsumer(controller, Vector.empty, now + keepAliveDelay) -> Sync[F].unit
          }
        case Status.Requesting(controller, BufferOrAwaiter.Buffer(buffered), _) =>
          if (buffered.size + 1 < config.prefetchMin) {
            leaseManager.manageLeases(in).map { token =>
              val todo = for {
                delay <- jitter((buffered.size + 1) * 10.millis) // TODO remove magic number
                _ <- Sync[F].sleep(delay)
                _ <- Sync[F].delay(controller.request(1))
              } yield ()
              Status.Requesting(controller, BufferOrAwaiter.Buffer[F, A](buffered :+ TokenedA(token, in)), None) -> todo.start.void
            }
          } else {
            for {
              now <- Sync[F].realTime
              keepAliveDelay <- jitter(config.progressTimeout)
              token <- leaseManager.manageLeases(in)
            } yield Status.AwaitingConsumer(controller, buffered :+ TokenedA(token, in), now + keepAliveDelay) -> Sync[F].unit
          }
        case status: Status.AwaitingConsumer[F, A] =>
          raiseForIllegalState(status, "handle stream error")
        case status: Status.Initializing[F, A] =>
          raiseForIllegalState(status, "handle stream error")
        case status: Status.Shutdown[F, A] =>
          raiseForIllegalState(status, "handle stream error")
      }
      .flatten

  def receiveController(controller: StreamController): F[Unit] =
    status.evalUpdate {
      case Status.Initializing(lastBackoff, receive) =>
        Sync[F].delay(controller.request(1)).as(Status.Requesting(controller, receive, Some(lastBackoff)))
      case status =>
        raiseForIllegalState(status, "receive controller")
    }

  private def keepAlive: F[Unit] =
    status
      .evalModify[FiniteDuration] {
        case Status.AwaitingConsumer(controller, buffered, keepAliveDue) =>
          Sync[F].realTime.flatMap { now =>
            if (now > keepAliveDue) {
              if (buffered.size < config.prefetchMax) {
                for {
                  _ <-
                    Logger[F].debug(
                      s"Requesting more messages from pubsub Streaming Pull $channelAffinity to avoid a timeout after ${config.progressTimeout} without activity. Buffer contains ${buffered.size} batches."
                    )
                  _ <- Sync[F].delay(controller.request(1))
                } yield Status.Requesting(controller, BufferOrAwaiter.Buffer[F, A](buffered), None) -> config.progressTimeout
              } else {
                for {
                  _ <-
                    Logger[F].info(
                      s"Dropping ${buffered.size} buffered batches from Streaming Pull $channelAffinity. Exceeded ${config.prefetchMax} pre-fetches while attempting to keep the stream alive."
                    )
                  _ <- Sync[F].delay(controller.cancel())
                  // This does IO, which is not ideal inside a `evalModify` block,
                  // but on balance it is ok here because we are handling an edge-case error scenario
                  _ <- leaseManager.stopManagingLeases(buffered.map(_.token))
                } yield Status.Shutdown(now) -> config.progressTimeout
              }
            } else {
              val nextTimeout = keepAliveDue - now
              Sync[F].pure(Status.AwaitingConsumer(controller, buffered, keepAliveDue) -> nextTimeout)
            }
          }
        case other =>
          Sync[F].pure(other -> config.progressTimeout)
      }
      .flatMap[Unit] { nextCheck =>
        Sync[F].sleep(nextCheck)
      }
      .foreverM
}

private object SourceCoordinator {

  case class TokenedA[A](token: Unique.Token, value: A)

  def resource[F[_]: Async, A](
    config: PubsubSourceConfigV2,
    streamManager: StreamManager[F],
    leaseManager: LeaseManager[F, A],
    channelAffinity: Int
  ): Resource[F, SourceCoordinator[F, A]] =
    Resource
      .eval(AtomicCell[F].of[Status[F, A]](Status.Shutdown(Duration.Zero)))
      .map { atomicCell =>
        new SourceCoordinator(config, atomicCell, streamManager, leaseManager, channelAffinity)
      }
      .flatTap(_.keepAlive.background)

  private sealed trait BufferOrAwaiter[F[_], A]
  private object BufferOrAwaiter {
    case class Awaiter[F[_], A](complete: DeferredSink[F, TokenedA[A]]) extends BufferOrAwaiter[F, A]
    case class Buffer[F[_], A](value: Vector[TokenedA[A]]) extends BufferOrAwaiter[F, A]
  }

  private sealed trait Status[F[_], A]
  private object Status {
    case class Shutdown[F[_], A](since: FiniteDuration) extends Status[F, A]

    case class Initializing[F[_], A](lastBackoff: FiniteDuration, receive: BufferOrAwaiter[F, A]) extends Status[F, A]

    case class Requesting[F[_], A](
      controller: StreamController,
      receive: BufferOrAwaiter[F, A],
      lastBackoff: Option[FiniteDuration]
    ) extends Status[F, A]

    case class AwaitingConsumer[F[_], A](
      controller: StreamController,
      buffered: Vector[TokenedA[A]],
      keepAliveDue: FiniteDuration
    ) extends Status[F, A]
  }
}
