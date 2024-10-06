/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import cats.effect.{Async, Deferred, Ref, Resource, Sync}
import cats.effect.std.Hotswap
import cats.effect.kernel.Unique
import cats.implicits._
import fs2.{Chunk, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

// pubsub
import com.google.api.gax.core.{ExecutorProvider, FixedExecutorProvider}
import com.google.api.gax.grpc.{ChannelPoolSettings, GrpcCallContext}
import com.google.api.gax.rpc.{ResponseObserver, StreamController}
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.pubsub.v1.{StreamingPullRequest, StreamingPullResponse}
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStub}
import io.grpc.Status
import org.threeten.bp.{Duration => ThreetenDuration}

// snowplow
import com.snowplowanalytics.snowplow.pubsub.GcpUserAgent
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}
import com.snowplowanalytics.snowplow.sources.pubsub.v2.PubsubRetryOps.implicits._

import scala.concurrent.duration.{DurationDouble, FiniteDuration}
import scala.jdk.CollectionConverters._

import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue}
import java.util.UUID

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
      .map { i =>
        val actionQueue = new LinkedBlockingQueue[SubscriberAction]()
        val clientId    = UUID.randomUUID
        val resource    = initializeStreamingPull(config, stub, actionQueue, i, clientId)
        Stream.resource(Hotswap(resource)).flatMap { case (hotswap, _) =>
          Stream
            .eval(pullFromQueue(config, actionQueue, refStates, hotswap, resource, i))
            .repeat
            .concurrently(extendDeadlines(config, stub, refStates, i))
        }
      }
      .parJoinUnbounded

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
        else
          toExtend.sortBy(_.currentDeadline).flatMap(_.ackIds).grouped(1000).toVector.traverse_ { ackIds =>
            Utils
              .modAck[F](config.subscription, stub, ackIds, config.durationPerAckExtension, channelAffinity)
              .retryingOnTransientGrpcFailures
              .recoveringOnGrpcInvalidArgument { s =>
                // This can happen if ack IDs were acked before we modAcked
                Logger[F].info(s"Ignoring error from GRPC when modifying ack IDs: ${s.getDescription}")
              }
          }
      }
      .repeat
      .drain

  /**
   * Pulls a SubscriberAction from a queue when one becomes available
   *
   * @param config
   *   The source configuration
   * @param queue
   *   The queue from which to pull a SubscriberAction
   * @param refStates
   *   A map from tokens to the data held about a batch of messages received from pubsub. This
   *   function must update the state to add new batches.
   * @param hotswap
   *   A Hotswap wrapping the Resource that is populating the queue
   * @param toSwap
   *   Initializes the Resource which is populating the queue. If we get an error from the queue
   *   then need to swap in the new Resource into the Hotswap
   * @param channelAffinity
   *   Identifies the GRPC channel (TCP connection) creating these Actions. Each GRPC channel has
   *   its own queue, observer, and puller.
   */
  private def pullFromQueue[F[_]: Async](
    config: PubsubSourceConfigV2,
    queue: LinkedBlockingQueue[SubscriberAction],
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]],
    hotswap: Hotswap[F, Unit],
    toSwap: Resource[F, Unit],
    channelAffinity: Int
  ): F[LowLevelEvents[Vector[Unique.Token]]] = {
    def go(delayOnSubscriberError: FiniteDuration): F[LowLevelEvents[Vector[Unique.Token]]] =
      resolveNextAction[F, SubscriberAction](queue).flatMap {
        case SubscriberAction.ProcessRecords(records, controller, timeReceived) =>
          val chunk = Chunk.from(records.map(_.getMessage.getData.asReadOnlyByteBuffer()))
          val (tstampSeconds, tstampNanos) =
            records.map(r => (r.getMessage.getPublishTime.getSeconds, r.getMessage.getPublishTime.getNanos)).min
          val ackIds = records.map(_.getAckId)
          for {
            token <- Unique[F].unique
            currentDeadline = timeReceived.plusMillis(config.durationPerAckExtension.toMillis)
            _ <- refStates.update(_ + (token -> PubsubBatchState(currentDeadline, ackIds, channelAffinity)))
            _ <- Sync[F].delay(controller.request(1))
          } yield LowLevelEvents(chunk, Vector(token), Some(Instant.ofEpochSecond(tstampSeconds, tstampNanos.toLong)))
        case SubscriberAction.SubscriberError(t) =>
          if (PubsubRetryOps.isRetryableException(t)) {
            val nextDelay = (2 * delayOnSubscriberError).min((10 + scala.util.Random.nextDouble()).second)
            // Log at debug level because retryable errors are very frequent.
            // In particular, if the pubsub subscription is empty then a streaming pull returns UNAVAILABLE
            Logger[F].debug(s"Retryable error on PubSub channel $channelAffinity: ${t.getMessage}") *>
              hotswap.clear *>
              Async[F].sleep(delayOnSubscriberError) *>
              hotswap.swap(toSwap) *>
              go(nextDelay)
          } else {
            Logger[F].error(t)("Exception from PubSub source") *> Sync[F].raiseError(t)
          }
      }

    go(delayOnSubscriberError = (1.0 + scala.util.Random.nextDouble()).second)
  }

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
        if (messages.isEmpty) {
          controller.request(1)
        } else {
          val action = SubscriberAction.ProcessRecords(messages, controller, Instant.now())
          actionQueue.put(action)
        }
      }

      override def onStart(c: StreamController): Unit = {
        controller = c
        controller.disableAutoInboundFlowControl()
        controller.request(1)
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
