/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import cats.effect.{Async, Deferred, Ref, Resource, Sync}
import cats.effect.std.{Hotswap, Queue, QueueSink}
import cats.effect.kernel.Unique
import cats.effect.implicits._
import cats.implicits._
import fs2.{Chunk, Pipe, Stream}
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

import scala.concurrent.duration.{Duration, DurationDouble, FiniteDuration}
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
      .map(i => miniPubsubStream(config, stub, refStates, i))
      .parJoinUnbounded

  private def miniPubsubStream[F[_]: Async](
    config: PubsubSourceConfigV2,
    stub: SubscriberStub,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]],
    channelAffinity: Int
  ): Stream[F, LowLevelEvents[Vector[Unique.Token]]] = {
    val jQueue   = new LinkedBlockingQueue[SubscriberAction]()
    val clientId = UUID.randomUUID
    val resource = initializeStreamingPull[F](config, stub, jQueue, channelAffinity, clientId)

    for {
      (hotswap, _) <- Stream.resource(Hotswap(resource))
      fs2Queue <- Stream.eval(Queue.synchronous[F, SubscriberAction])
      _ <- extendDeadlines(config, stub, refStates, channelAffinity).spawn
      _ <- Stream.eval(queueToQueue(config, jQueue, fs2Queue, stub, channelAffinity)).repeat.spawn
      lle <- Stream
               .fromQueueUnterminated(fs2Queue)
               .through(toLowLevelEvents(config, refStates, hotswap, resource, channelAffinity))
    } yield lle
  }

  private def queueToQueue[F[_]: Async](
    config: PubsubSourceConfigV2,
    jQueue: LinkedBlockingQueue[SubscriberAction],
    fs2Queue: QueueSink[F, SubscriberAction],
    stub: SubscriberStub,
    channelAffinity: Int
  ): F[Unit] =
    resolveNextAction(jQueue).flatMap {
      case action @ SubscriberAction.ProcessRecords(records, controller, _) =>
        val fallback = if (config.modackOnProgressTimeout) {
          val ackIds = records.map(_.getAckId)
          if (config.cancelOnProgressTimeout)
            Logger[F].debug(s"Cancelling Pubsub channel $channelAffinity for not making progress") *>
              Sync[F].delay(controller.cancel()) *> Utils.modAck(config.subscription, stub, ackIds, Duration.Zero, channelAffinity)
          else
            Logger[F].debug(s"Nacking on Pubsub channel $channelAffinity for not making progress") *>
              Sync[F].delay(controller.request(1)) *> Utils.modAck(config.subscription, stub, ackIds, Duration.Zero, channelAffinity)
        } else {
          if (config.cancelOnProgressTimeout)
            Logger[F].debug(s"Cancelling Pubsub channel $channelAffinity for not making progress") *>
              Sync[F].delay(controller.cancel()) *> fs2Queue.offer(action)
          else
            fs2Queue.offer(action)
        }
        fs2Queue.offer(action).timeoutTo(config.progressTimeout, fallback)
      case action: SubscriberAction.SubscriberError =>
        fs2Queue.offer(action)
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

  /**
   * Pipe from SubscriberAction to LowLevelEvents TODO: Say what else this does
   *
   * @param config
   *   The source configuration
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
  private def toLowLevelEvents[F[_]: Async](
    config: PubsubSourceConfigV2,
    refStates: Ref[F, Map[Unique.Token, PubsubBatchState]],
    hotswap: Hotswap[F, Unit],
    toSwap: Resource[F, Unit],
    channelAffinity: Int
  ): Pipe[F, SubscriberAction, LowLevelEvents[Vector[Unique.Token]]] =
    _.flatMap {
      case SubscriberAction.ProcessRecords(records, controller, timeReceived) =>
        val chunk = Chunk.from(records.map(_.getMessage.getData.asReadOnlyByteBuffer()))
        val (tstampSeconds, tstampNanos) =
          records.map(r => (r.getMessage.getPublishTime.getSeconds, r.getMessage.getPublishTime.getNanos)).min
        val ackIds = records.map(_.getAckId)
        Stream.eval {
          for {
            token <- Unique[F].unique
            currentDeadline = timeReceived.plusMillis(config.durationPerAckExtension.toMillis)
            _ <- refStates.update(_ + (token -> PubsubBatchState(currentDeadline, ackIds, channelAffinity)))
            _ <- Sync[F].delay(controller.request(1))
          } yield LowLevelEvents(chunk, Vector(token), Some(Instant.ofEpochSecond(tstampSeconds, tstampNanos.toLong)))
        }
      case SubscriberAction.SubscriberError(t) =>
        if (PubsubRetryOps.isRetryableException(t)) {
          // val nextDelay = (2 * delayOnSubscriberError).min((10 + scala.util.Random.nextDouble()).second)
          // Log at debug level because retryable errors are very frequent.
          // In particular, if the pubsub subscription is empty then a streaming pull returns UNAVAILABLE
          Stream.eval {
            Logger[F].debug(s"Retryable error on PubSub channel $channelAffinity: ${t.getMessage}") *>
              hotswap.clear *>
              Async[F].sleep((1.0 + scala.util.Random.nextDouble()).second) *> // TODO expotential backoff
              hotswap.swap(toSwap)
          }.drain
        } else if (t.isInstanceOf[java.util.concurrent.CancellationException]) {
          Stream.eval {
            Logger[F].debug("Cancellation exception on PubSub channel") *>
              hotswap.clear *>
              hotswap.swap(toSwap)
          }.drain
        } else {
          Stream.eval(Logger[F].error(t)("Exception from PubSub source")) *> Stream.raiseError[F](t)
        }
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
      .setClientId(if (config.consistentClientId) clientId.toString else UUID.randomUUID.toString)
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
