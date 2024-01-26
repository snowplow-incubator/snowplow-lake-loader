/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.sinks

import cats.implicits._
import cats.effect.{Async, Sync}
import cats.effect.kernel.Resource
import cats.effect.implicits._

import com.google.api.gax.batching.BatchingSettings
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{ProjectTopicName, PubsubMessage}
import com.google.api.core.ApiFutures
import org.threeten.bp.{Duration => ThreetenDuration}

import java.util.UUID
import scala.jdk.CollectionConverters._

import com.snowplowanalytics.snowplow.pubsub.FutureInterop

object PubsubSink {

  def resource[F[_]: Async](config: PubsubSinkConfig): Resource[F, Sink[F]] =
    mkPublisher[F](config).map { p =>
      Sink(sinkBatch[F](p, _))
    }

  private def sinkBatch[F[_]: Async](publisher: Publisher, batch: List[Sinkable]): F[Unit] =
    batch
      .parTraverse { case Sinkable(bytes, _, attributes) =>
        for {
          uuid <- Async[F].delay(UUID.randomUUID)
          message = PubsubMessage.newBuilder
                      .setData(ByteString.copyFrom(bytes))
                      .setMessageId(uuid.toString)
                      .putAllAttributes(attributes.asJava)
                      .build
          fut <- Async[F].delay(publisher.publish(message))
        } yield fut
      }
      .flatMap { futures =>
        for {
          _ <- Async[F].delay(publisher.publishAllOutstanding)
          combined = ApiFutures.allAsList(futures.asJava)
          _ <- FutureInterop.fromFuture(combined)
        } yield ()
      }

  private def mkPublisher[F[_]: Sync](config: PubsubSinkConfig): Resource[F, Publisher] = {
    val topic = ProjectTopicName.of(config.topic.projectId, config.topic.topicId)

    val batchSettings = BatchingSettings.newBuilder
      .setElementCountThreshold(config.batchSize)
      .setRequestByteThreshold(config.requestByteThreshold)
      .setDelayThreshold(ThreetenDuration.ofNanos(Long.MaxValue))

    val make = Sync[F].delay {
      Publisher
        .newBuilder(topic)
        .setBatchingSettings(batchSettings.build)
        .build
    }

    Resource.make(make) { publisher =>
      Sync[F].blocking {
        publisher.shutdown()
      }
    }
  }
}
