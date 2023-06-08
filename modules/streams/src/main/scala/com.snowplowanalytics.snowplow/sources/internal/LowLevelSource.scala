package com.snowplowanalytics.snowplow.sources.internal

import cats.Monad
import cats.implicits._
import cats.effect.std.{MapRef, Queue}
import cats.effect.kernel.Unique
import cats.effect.{Async, Concurrent, Sync}
import fs2.{Pipe, Pull, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}

/**
 * A common interface over external sources of events
 *
 * This library uses [[LowLevelSource]] internally as a stepping stone towards implementing a
 * [[SourceAndAckk]].
 *
 * @tparam F
 *   An IO effect type
 * @tparam C
 *   A source-specific thing which can be checkpointed
 */
private[sources] trait LowLevelSource[F[_], C] {

  def checkpointer: Checkpointer[F, C]

  /**
   * Provides a stream of stream of low level events
   *
   * The inner streams are processed one at a time, with clean separation before starting the next
   * inner stream. This is required e.g. for Kafka, where the end of a stream represents client
   * rebalancing.
   *
   * A new [[EventProcessor]] will be invoked for each inner stream
   */
  def stream: Stream[F, Stream[F, LowLevelEvents[C]]]

}

private[sources] object LowLevelSource {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  /**
   * Lifts the internal [[LowLevelSource]] into a [[SourceAndAck]], which is the public API of this
   * library
   */
  def toSourceAndAck[F[_]: Async, C](source: LowLevelSource[F, C]): SourceAndAck[F] = new SourceAndAck[F] {
    def stream(config: EventProcessingConfig, processor: EventProcessor[F]): Stream[F, Nothing] =
      source.stream.flatMap { s2 =>
        Stream.eval(MapRef.ofSingleImmutableMap[F, Unique.Token, C]()).flatMap { ref =>
          s2.through(tokened(ref))
            .through(windowed(config.windowing))
            .through(eagerWindows(CleanCancellation(messageSink(processor, ref, source.checkpointer))))
        }
      }
  }

  /**
   * An fs2 Pipe which caches the low-level checkpointable item, and replaces it with a token.
   *
   * The token can later be exchanged for the original checkpointable item
   */
  private def tokened[F[_]: Unique: Monad, C](ref: MapRef[F, Unique.Token, Option[C]]): Pipe[F, LowLevelEvents[C], TokenedEvents] =
    _.evalMap { case LowLevelEvents(events, ack) =>
      for {
        token <- Unique[F].unique
        _ <- ref(token).set(Some(ack))
      } yield TokenedEvents(events, token)
    }

  /**
   * An fs2 Pipe which feeds tokened messages into the [[EventProcessor]] and invokes the
   * checkpointer once they are processed
   *
   * @tparam F
   *   The effect type
   * @tparam C
   *   A checkpointable item, speficic to the stream
   * @param processor
   *   the [[EventProcessor]] provided by the application (e.g. Enrich or Transformer)
   * @param ref
   *   A map from tokens to checkpointable items. When the [[EventProcessor]] emits a token to
   *   signal completion of a batch of events, then we look up the checkpointable item from this
   *   map.
   * @param checkpointer
   *   Actions a ack/checkpoint when given a checkpointable action
   */
  private def messageSink[F[_]: Async, C](
    processor: EventProcessor[F],
    ref: MapRef[F, Unique.Token, Option[C]],
    checkpointer: Checkpointer[F, C]
  ): Pipe[F, TokenedEvents, Nothing] =
    _.evalTap { case TokenedEvents(events, _) =>
      Logger[F].debug(s"Batch of ${events.size} events received from the source stream")
    }
      .through(processor)
      .chunks
      .prefetch // This prefetch means we can ack messages concurrently with processing the next batch
      .evalMap { chunk =>
        chunk.iterator.toSeq
          .traverse { token =>
            ref(token).getAndSet(None).flatMap {
              case Some(c) => Async[F].pure(c)
              case None => Async[F].raiseError[C](new IllegalStateException("Missing checkpoint for token"))
            }
          }
          .flatMap { cs =>
            checkpointer.checkpoint(checkpointer.combineAll(cs))
          }
      }
      .drain

  private def eagerWindows[F[_]: Concurrent, A](sink: Pipe[F, A, Nothing]): Pipe[F, Stream[F, A], Nothing] =
    _.map(sink).prefetch // This prefetch means we start processing the next window while the previous window is still finishing up.
      .flatten.drain

  private def windowed[F[_]: Async, A](config: EventProcessingConfig.Windowing): Pipe[F, A, Stream[F, A]] =
    config match {
      case EventProcessingConfig.NoWindowing => in => Stream.emit(in)
      case tw: EventProcessingConfig.TimedWindows => timedWindows(tw)
    }

  /**
   * An fs2 Pipe which converts a stream of `A` into a stream of `Stream[F, A]`. Each stream in the
   * output provides events over a fixed window of time. When the window is over, the inner stream
   * terminates with success.
   *
   * For an [[EventProcessor]], termination of the inner stream is a signal to cleanly handle the
   * end of a window. For example, the AWS Transformer would handle termination of the inner stream
   * by flushing all pending events to S3 and then sending the SQS message.
   */
  private def timedWindows[F[_]: Async, A](config: EventProcessingConfig.TimedWindows): Pipe[F, A, Stream[F, A]] = {
    def go(timedPull: Pull.Timed[F, A], current: Option[Queue[F, Option[A]]]): Pull[F, Stream[F, A], Unit] =
      timedPull.uncons.flatMap {
        case None =>
          current match {
            case None => Pull.done
            case Some(q) => Pull.eval(q.offer(None)) >> Pull.done
          }
        case Some((Left(_), next)) =>
          val openWindow =
            Pull.eval(Logger[F].info(s"Opening new window with duration ${config.duration}")) >> next.timeout(config.duration)
          current match {
            case None => openWindow >> go(next, None)
            case Some(q) => openWindow >> Pull.eval(q.offer(None)) >> go(next, None)
          }
        case Some((Right(chunk), next)) =>
          current match {
            case None =>
              for {
                q <- Pull.eval(Queue.synchronous[F, Option[A]])
                _ <- Pull.output1(Stream.fromQueueNoneTerminated(q))
                _ <- Pull.eval(chunk.traverse(a => q.offer(Some(a))))
                _ <- go(next, Some(q))
              } yield ()
            case Some(q) =>
              Pull.eval(chunk.traverse(a => q.offer(Some(a)))) >> go(next, Some(q))
          }
      }

    in =>
      in.pull
        .timed { timedPull: Pull.Timed[F, A] =>
          val timeout = timeoutForFirstWindow(config)
          for {
            _ <- Pull.eval(Logger[F].info(s"Opening first window with randomly adjusted duration of $timeout"))
            _ <- timedPull.timeout(timeout)
            _ <- go(timedPull, None)
          } yield ()
        }
        .stream
        .prefetch // This prefetch is required to pull items into the emitted stream
  }

  /**
   * When the application first starts up, the first timed window should have a random size.
   *
   * This addresses the situation where several parallel instances of the app all start at the same
   * time. All instances in the group should end windows at slightly different times, so that
   * downstream gets a more steady flow of completed batches.
   */
  private def timeoutForFirstWindow(config: EventProcessingConfig.TimedWindows) =
    (config.duration.toMillis * config.firstWindowScaling).toLong.milliseconds
}
