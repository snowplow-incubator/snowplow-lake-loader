/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import cats.effect.{Async, Deferred, Ref, Resource, Sync}
import cats.effect.kernel.Unique
import cats.implicits._
import cats.effect.implicits._
import fs2.{Chunk, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

// pubsub
import com.google.api.gax.core.{ExecutorProvider, FixedExecutorProvider}
import com.google.api.gax.grpc.ChannelPoolSettings
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStub}
import org.threeten.bp.{Duration => ThreetenDuration}

// snowplow
import com.snowplowanalytics.snowplow.pubsub.GcpUserAgent
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue}

object PubsubSourceV2 {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: PubsubSourceConfigV2): F[SourceAndAck[F]] =
    Deferred[F, PubsubCheckpointer.Resources[F]].flatMap { deferred =>
      LowLevelSource.toSourceAndAck(lowLevel(config, deferred))
    }

  private def lowLevel[F[_]: Async](
    config: PubsubSourceConfigV2,
    deferredResources: Deferred[F, PubsubCheckpointer.Resources[F]]
  ): LowLevelSource[F, Vector[Unique.Token]] =
    new LowLevelSource[F, Vector[Unique.Token]] {
      def checkpointer: Checkpointer[F, Vector[Unique.Token]] = new PubsubCheckpointer(config.subscription, deferredResources)

      def stream: Stream[F, Stream[F, LowLevelEvents[Vector[Unique.Token]]]] =
        pubsubStream(config, deferredResources)

      def lastLiveness: F[FiniteDuration] =
        Sync[F].realTime
    }

  private def pubsubStream[F[_]: Async](
    config: PubsubSourceConfigV2,
    deferredResources: Deferred[F, PubsubCheckpointer.Resources[F]]
  ): Stream[F, Stream[F, LowLevelEvents[Vector[Unique.Token]]]] =
    for {
      parallelPullCount <- Stream.eval(Sync[F].delay(chooseNumParallelPulls(config)))
      channelCount = chooseNumTransportChannels(config, parallelPullCount)
      stub <- Stream.resource(stubResource(config, channelCount))
      refStates <- Stream.eval(Ref[F].of(Map.empty[Unique.Token, PubsubBatchState]))
      _ <- Stream.bracket(Sync[F].unit)(_ => nackRefStatesForShutdown(config, stub, refStates))
      _ <- Stream.eval(deferredResources.complete(PubsubCheckpointer.Resources(stub, refStates)))
    } yield Stream
      .range(0, parallelPullCount)
      .map(i => miniPubsubStream(config, stub, refStates, i))
      .parJoinUnbounded

  private def miniPubsubStream[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]],
    channelAffinity: Int
  ): Stream[F, LowLevelEvents[Vector[Unique.Token]]] =
    for {
      jQueue <- Stream.emit(new LinkedBlockingQueue[SubscriberAction]())
      _ <- Stream.bracket(Sync[F].unit)(_ => nackQueueForShutdown(config, stub, jQueue, channelAffinity))
      streamManager <- Stream.resource(StreamManager.resource(config, stub, jQueue, channelAffinity))
      leaseManager <- Stream.resource(LeaseManager.resource(config, stub, refStates, channelAffinity))
      sourceCoordinator <- Stream.resource(SourceCoordinator.resource(config, streamManager, leaseManager, channelAffinity))
      _ <- pullFromQueue(jQueue, sourceCoordinator, channelAffinity).spawn
      tokenedAction <- Stream.eval(sourceCoordinator.pull).repeat
    } yield {
      val SourceCoordinator.TokenedA(token, SubscriberAction.ProcessRecords(records, _)) = tokenedAction
      val chunk = Chunk.from(records.toVector.map(_.getMessage.getData.asReadOnlyByteBuffer()))
      val (tstampSeconds, tstampNanos) =
        records.toVector.map(r => (r.getMessage.getPublishTime.getSeconds, r.getMessage.getPublishTime.getNanos)).min
      LowLevelEvents(chunk, Vector(token), Some(Instant.ofEpochSecond(tstampSeconds, tstampNanos.toLong)))
    }

  private def pullFromQueue[F[_]: Sync](
    queue: LinkedBlockingQueue[SubscriberAction],
    sourceCoordinator: SourceCoordinator[F, SubscriberAction.ProcessRecords],
    channelAffinity: Int
  ): Stream[F, Nothing] =
    Stream
      .eval {
        Sync[F].uncancelable { poll =>
          poll(resolveNextAction(queue))
            .flatMap {
              case SubscriberAction.Ready(controller) =>
                sourceCoordinator.receiveController(controller)
              case processRecords: SubscriberAction.ProcessRecords =>
                sourceCoordinator.receiveItem(processRecords)
              case SubscriberAction.SubscriberError(t) =>
                if (PubsubRetryOps.isRetryableException(t)) {
                  // Log at debug level because retryable errors are very frequent.
                  // In particular, if the pubsub subscription is empty then a streaming pull returns UNAVAILABLE
                  Logger[F].debug(s"Retryable error on PubSub channel $channelAffinity: ${t.getMessage}") >>
                    sourceCoordinator.handleStreamError
                } else if (t.isInstanceOf[java.util.concurrent.CancellationException]) {
                  // The SourceCoordinator caused this by cancelling the stream.
                  // No need to inform the SourceCoordinator.
                  Logger[F].debug("Cancellation exception on PubSub channel")
                } else {
                  Logger[F].error(t)("Exception from PubSub source") >> Sync[F].raiseError[Unit](t)
                }
            }
        }
      }
      .repeat
      .drain

  private def resolveNextAction[F[_]: Sync, A](queue: LinkedBlockingQueue[A]): F[A] =
    Sync[F].delay(Option[A](queue.poll)).flatMap {
      case Some(action) => Sync[F].pure(action)
      case None         => Sync[F].interruptible(queue.take)
    }

  private def stubResource[F[_]: Async](
    config: PubsubSourceConfigV2,
    channelCount: Int
  ): Resource[F, SubscriberStub] =
    for {
      executor <- executorResource(Sync[F].delay(Executors.newScheduledThreadPool(2)))
      subStub <- buildSubscriberStub(config, channelCount, FixedExecutorProvider.create(executor))
    } yield subStub

  private def buildSubscriberStub[F[_]: Sync](
    config: PubsubSourceConfigV2,
    channelCount: Int,
    executorProvider: ExecutorProvider
  ): Resource[F, GrpcSubscriberStub] = {
    val channelProvider = SubscriptionAdminSettings
      .defaultGrpcTransportProviderBuilder()
      .setMaxInboundMessageSize(20 << 20)
      .setMaxInboundMetadataSize(20 << 20)
      .setKeepAliveTime(ThreetenDuration.ofMinutes(5))
      .setChannelPoolSettings {
        ChannelPoolSettings.staticallySized(channelCount)
      }
      .build

    val stubSettings = SubscriberStubSettings
      .newBuilder()
      .setBackgroundExecutorProvider(executorProvider)
      .setCredentialsProvider(SubscriptionAdminSettings.defaultCredentialsProviderBuilder().build())
      .setTransportChannelProvider(channelProvider)
      .setHeaderProvider(GcpUserAgent.headerProvider(config.gcpUserAgent))
      .setEndpoint(SubscriberStubSettings.getDefaultEndpoint())
      .build

    Resource.make(Sync[F].delay(GrpcSubscriberStub.create(stubSettings)))(stub => Sync[F].blocking(stub.shutdownNow))
  }

  private def nackRefStatesForShutdown[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]]
  ): F[Unit] =
    refStates.getAndSet(Map.empty).flatMap { m =>
      m.values.groupBy(_.channelAffinity).toVector.parTraverse_ { case (channelAffinity, batches) =>
        Utils.modAck(config.subscription, stub, batches.flatMap(_.ackIds.toVector).toVector, Duration.Zero, channelAffinity)
      }
    }

  private def nackQueueForShutdown[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    queue: LinkedBlockingQueue[SubscriberAction],
    channelAffinity: Int
  ): F[Unit] = {
    val ackIds = queue.iterator.asScala.toVector.flatMap {
      case SubscriberAction.ProcessRecords(records, _) =>
        records.toVector.map(_.getAckId)
      case _ =>
        Vector.empty
    }
    Utils.modAck(config.subscription, stub, ackIds, Duration.Zero, channelAffinity)
  }

  private def executorResource[F[_]: Sync, E <: ExecutorService](make: F[E]): Resource[F, E] =
    Resource.make(make)(es => Sync[F].blocking(es.shutdown()))

  /**
   * Converts `parallelPullFactor` to a suggested number of parallel pulls
   *
   * For bigger instances (more cores) the downstream processor can typically process events more
   * quickly. So the PubSub subscriber needs more parallelism in order to keep downstream saturated
   * with events.
   */
  private def chooseNumParallelPulls(config: PubsubSourceConfigV2): Int =
    (Runtime.getRuntime.availableProcessors * config.parallelPullFactor)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

  /**
   * Picks a sensible number of GRPC transport channels (roughly equivalent to a TCP connection)
   *
   * GRPC has a hard limit of 100 concurrent RPCs on a channel. And experience shows it is healthy
   * to stay much under that limit. If we need to open a large number of streaming pulls then we
   * might approach/exceed that limit.
   */
  private def chooseNumTransportChannels(config: PubsubSourceConfigV2, parallelPullCount: Int): Int =
    (BigDecimal(parallelPullCount) / config.maxPullsPerTransportChannel)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt

}
