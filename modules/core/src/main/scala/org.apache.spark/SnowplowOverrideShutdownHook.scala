package org.apache.spark

import cats.implicits._
import cats.effect.{Async, Deferred, Sync}
import cats.effect.std.Dispatcher
import cats.effect.kernel.Resource
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import org.apache.spark.util.ShutdownHookManager

/**
 * This is needed to interrupt and override Spark's default behaviour of shutting down the
 * SparkContext immediately after receiving a SIGINT
 *
 * We manage our own graceful termination, so Spark's default behaviour gets in our way
 */
object SnowplowOverrideShutdownHook {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def resource[F[_]: Async]: Resource[F, Unit] =
    for {
      dispatcher <- Dispatcher.sequential(await = true)
      sig <- Resource.make(Deferred[F, Unit])(_.complete(()) >> Async[F].cede)
      _ <- Resource.eval(addShutdownHook(dispatcher, sig))
    } yield ()

  private def addShutdownHook[F[_]: Sync](dispatcher: Dispatcher[F], sig: Deferred[F, Unit]): F[Unit] =
    Sync[F].delay {
      ShutdownHookManager.addShutdownHook(999) { () =>
        dispatcher.unsafeRunSync {
          Logger[F].info("Interrupted Spark's shutdown hook") >>
            sig.get
        }
      }
    }.void

}
