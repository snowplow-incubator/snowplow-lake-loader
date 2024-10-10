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
import fs2.{Chunk, Pipe, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

// pubsub
import com.google.api.gax.core.{ExecutorProvider, FixedExecutorProvider}
import com.google.api.gax.grpc.{ChannelPoolSettings, GrpcCallContext}
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.pubsub.v1.{PullRequest, PullResponse}
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStub}
import org.threeten.bp.{Duration => ThreetenDuration}

// snowplow
import com.snowplowanalytics.snowplow.pubsub.GcpUserAgent
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}
import com.snowplowanalytics.snowplow.sources.pubsub.v2.PubsubRetryOps.implicits._

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import java.util.concurrent.{ExecutorService, Executors}

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
    Stream
      .eval[F, PullResponse](pullFromSubscription(config, stub, channelAffinity))
      .filter(_.getReceivedMessagesCount > 0)
      .through(addToRefStates(config, stub, refStates, channelAffinity))
      .repeat
      .prefetch
      .concurrently(extendDeadlines(config, stub, refStates, channelAffinity))

  private def pullFromSubscription[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    channelAffinity: Int
  ): F[PullResponse] = {
    val context = GrpcCallContext.createDefault.withChannelAffinity(channelAffinity)
    val request = PullRequest.newBuilder
      .setSubscription(config.subscription.show)
      .setMaxMessages(config.maxMessagesPerPull)
      .build
    val io = for {
      apiFuture <- Sync[F].delay(stub.pullCallable.futureCall(request, context))
      res <- FutureInterop.fromFuture[F, PullResponse](apiFuture)
    } yield res
    io.retryingOnTransientGrpcFailures
  }

  private def addToRefStates[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]],
    channelAffinity: Int
  ): Pipe[F, PullResponse, LowLevelEvents[Vector[Unique.Token]]] =
    _.evalMap { response =>
      val records = response.getReceivedMessagesList.asScala.toVector
      val chunk   = Chunk.from(records.map(_.getMessage.getData.asReadOnlyByteBuffer()))
      val (tstampSeconds, tstampNanos) =
        records.map(r => (r.getMessage.getPublishTime.getSeconds, r.getMessage.getPublishTime.getNanos)).min
      val ackIds = records.map(_.getAckId)
      for {
        timeReceived <- Sync[F].realTimeInstant
        _ <- Utils.modAck[F](config.subscription, stub, ackIds, config.durationPerAckExtension, channelAffinity)
        token <- Unique[F].unique
        currentDeadline = timeReceived.plusMillis(config.durationPerAckExtension.toMillis)
        _ <- refStates.update(_ + (token -> PubsubBatchState(currentDeadline, ackIds, channelAffinity)))
      } yield LowLevelEvents(chunk, Vector(token), Some(Instant.ofEpochSecond(tstampSeconds, tstampNanos.toLong)))
    }

  /**
   * Modify ack deadlines if we need more time to process the messages
   *
   * @param config
   *   The Source configuration
   * @param stub
   *   The GRPC stub on which we can issue modack requests
   * @param refStates
   *   A map from tokens to the data held about a batch of messages received from pubsub. This
   *   function must update the state if it extends a deadline.
   * @param channelAffinity
   *   Identifies the GRPC channel (TCP connection) creating these Actions. Each GRPC channel has
   *   its own concurrent stream modifying the ack deadlines.
   */
  private def extendDeadlines[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]],
    channelAffinity: Int
  ): Stream[F, Nothing] =
    Stream
      .eval(Sync[F].realTimeInstant)
      .evalMap { now =>
        val minAllowedDeadline = now.plusMillis((config.minRemainingDeadline * config.durationPerAckExtension.toMillis).toLong)
        val newDeadline        = now.plusMillis(config.durationPerAckExtension.toMillis)
        refStates.modify { m =>
          val toExtend = m.filter { case (_, batchState) =>
            batchState.channelAffinity === channelAffinity && batchState.currentDeadline.isBefore(minAllowedDeadline)
          }
          val fixed = toExtend.view
            .mapValues(_.copy(currentDeadline = newDeadline))
            .toMap
          (m ++ fixed, toExtend.values.toVector)
        }
      }
      .evalMap { toExtend =>
        if (toExtend.isEmpty)
          Sync[F].sleep(0.5 * config.minRemainingDeadline * config.durationPerAckExtension)
        else {
          val ackIds = toExtend.sortBy(_.currentDeadline).flatMap(_.ackIds)
          Utils.modAck[F](config.subscription, stub, ackIds, config.durationPerAckExtension, channelAffinity)
        }
      }
      .repeat
      .drain

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
