package com.snowplowanalytics.snowplow.sources

import cats.Applicative
import cats.kernel.Semigroup
import cats.implicits._
import cats.effect.{Async, Sync}
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

// kafka
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

// snowplow
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sources.internal.{Checkpointer, LowLevelEvents, LowLevelSource}

object KafkaSource {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: KafkaSourceConfig): SourceAndAck[F] =
    LowLevelSource.toSourceAndAck(lowLevel(config))

  private def lowLevel[F[_]: Async](config: KafkaSourceConfig): LowLevelSource[F, KafkaCheckpoints[F]] =
    new LowLevelSource[F, KafkaCheckpoints[F]] {
      def checkpointer: Checkpointer[F, KafkaCheckpoints[F]] = kafkaCheckpointer

      def stream: Stream[F, Stream[F, LowLevelEvents[KafkaCheckpoints[F]]]] =
        kafkaStream(config)
    }

  case class OffsetAndCommit[F[_]](offset: Long, commit: F[Unit])
  case class KafkaCheckpoints[F[_]](byPartition: Map[Int, OffsetAndCommit[F]])

  private implicit def offsetAndCommitSemigroup[F[_]]: Semigroup[OffsetAndCommit[F]] = new Semigroup[OffsetAndCommit[F]] {
    def combine(x: OffsetAndCommit[F], y: OffsetAndCommit[F]): OffsetAndCommit[F] =
      if (x.offset > y.offset) x else y
  }

  private def kafkaCheckpointer[F[_]: Applicative]: Checkpointer[F, KafkaCheckpoints[F]] = new Checkpointer[F, KafkaCheckpoints[F]] {
    def combine(x: KafkaCheckpoints[F], y: KafkaCheckpoints[F]): KafkaCheckpoints[F] =
      KafkaCheckpoints(x.byPartition |+| y.byPartition)

    val empty: KafkaCheckpoints[F] = KafkaCheckpoints(Map.empty)
    def checkpoint(c: KafkaCheckpoints[F]): F[Unit] = c.byPartition.values.toSeq.traverse_(_.commit)
  }

  private def kafkaStream[F[_]: Async](config: KafkaSourceConfig): Stream[F, Stream[F, LowLevelEvents[KafkaCheckpoints[F]]]] =
    KafkaConsumer
      .stream(consumerSettings[F](config))
      .evalTap(_.subscribeTo(config.topicName))
      .flatMap { consumer =>
        consumer.partitionsMapStream
          .evalMapFilter(logWhenNoPartitions[F])
          .map(joinPartitions[F](_))
      }

  private type PartitionedStreams[F[_]] = Map[TopicPartition, Stream[F, CommittableConsumerRecord[F, Array[Byte], Array[Byte]]]]

  private def joinPartitions[F[_]: Async](
    partitioned: PartitionedStreams[F]
  ): Stream[F, LowLevelEvents[KafkaCheckpoints[F]]] = {
    val streams = partitioned.toSeq.map { case (topicPartition, stream) =>
      stream.chunks
        .flatMap { chunk =>
          chunk.last match {
            case Some(last) =>
              val events = chunk.iterator.map {
                _.record.value
              }.toList
              val ack = KafkaCheckpoints(Map(topicPartition.partition -> OffsetAndCommit(last.record.offset, last.offset.commit)))
              Stream.emit(LowLevelEvents(events, ack))
            case None =>
              Stream.empty
          }
        }
    }

    val formatted = formatForLog(partitioned.keys)

    Stream.eval(Logger[F].info(s"Processsing partitions: $formatted")).drain ++
      Stream
        .emits(streams)
        .parJoinUnbounded
        .onFinalize {
          Logger[F].info(s"Stopping processing of partitions: $formatted")
        }
  }

  private def logWhenNoPartitions[F[_]: Sync](partitioned: PartitionedStreams[F]): F[Option[PartitionedStreams[F]]] =
    if (partitioned.isEmpty)
      Logger[F].info("No partitions are currently assigned to this processor").as(None)
    else
      Sync[F].pure(Some(partitioned))

  def formatForLog(tps: Iterable[TopicPartition]): String =
    tps
      .map { tp =>
        s"${tp.topic}-${tp.partition}"
      }
      .toSeq
      .sorted
      .mkString(",")

  private def consumerSettings[F[_]: Async](config: KafkaSourceConfig): ConsumerSettings[F, Array[Byte], Array[Byte]] =
    ConsumerSettings[F, Array[Byte], Array[Byte]]
      .withBootstrapServers(config.bootstrapServers)
      .withGroupId(config.groupId)
      .withEnableAutoCommit(false)
      .withAllowAutoCreateTopics(false)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withProperties(
        ("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"),
        ("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      )

}
