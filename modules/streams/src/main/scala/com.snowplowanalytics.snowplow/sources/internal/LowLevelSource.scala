package com.snowplowanalytics.snowplow.sources.internal

import cats.Monad
import cats.implicits._
import cats.effect.std.{MapRef, Queue, Random}
import cats.effect.kernel.Unique
import cats.effect.{Async, Concurrent, Sync}
import fs2.{Pipe, Pull, Stream}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.{DurationLong, FiniteDuration}

import com.snowplowanalytics.snowplow.sources.{EventProcessor, SourceAndAck, SourceConfig, TokenedEvents}

trait LowLevelSource[F[_], C] {

  def checkpointer: Checkpointer[F, C]

  def stream: Stream[F, Stream[F, LowLevelEvents[C]]]

}

object LowLevelSource {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def toSourceAndAck[F[_]: Async, C](source: LowLevelSource[F, C]): SourceAndAck[F] = new SourceAndAck[F] {
    def stream(config: SourceConfig, processor: EventProcessor[F]): Stream[F, Nothing] =
      source.stream.flatMap { s2 =>
        Stream.eval(MapRef.ofSingleImmutableMap[F, Unique.Token, C]()).flatMap { ref =>
          s2.through(tokened(ref))
            .through(windowed(config.windowing))
            .through(eagerWindows(CleanCancellation(messageSink(processor, ref, source.checkpointer))))
        }
      }
  }

  private def tokened[F[_]: Unique: Monad, C](ref: MapRef[F, Unique.Token, Option[C]]): Pipe[F, LowLevelEvents[C], TokenedEvents] =
    _.evalMap { case LowLevelEvents(events, ack) =>
      for {
        token <- Unique[F].unique
        _ <- ref(token).set(Some(ack))
      } yield TokenedEvents(events, token)
    }

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
      .prefetch // This prefetch means we can ack messages concurrently with sinking the next batch
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

  private def windowed[F[_]: Async, A](config: SourceConfig.Windowing): Pipe[F, A, Stream[F, A]] =
    config match {
      case SourceConfig.NoWindowing => in => Stream.emit(in)
      case SourceConfig.TimedWindows(duration) => timedWindows(duration)
    }

  private def timedWindows[F[_]: Async, A](duration: FiniteDuration): Pipe[F, A, Stream[F, A]] = {
    def go(timedPull: Pull.Timed[F, A], current: Option[Queue[F, Option[A]]]): Pull[F, Stream[F, A], Unit] =
      timedPull.uncons.flatMap {
        case None =>
          current match {
            case None => Pull.done
            case Some(q) => Pull.eval(q.offer(None)) >> Pull.done
          }
        case Some((Left(_), next)) =>
          val openWindow = Pull.eval(Logger[F].info(s"Opening new window with duration $duration")) >> next.timeout(duration)
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
          for {
            timeout <- Pull.eval(adjustFirstWindow(duration))
            _ <- Pull.eval(Logger[F].info(s"Opening first window with randomly adjusted duration of $timeout"))
            _ <- timedPull.timeout(timeout)
            _ <- go(timedPull, None)
          } yield ()
        }
        .stream
        .prefetch // This prefetch is required to pull items into the emitted stream
  }

  private def adjustFirstWindow[F[_]: Async](duration: FiniteDuration): F[FiniteDuration] =
    for {
      random <- Random.scalaUtilRandom
      factor <- random.nextDouble
    } yield (duration.toMillis * factor).toLong.milliseconds
}
