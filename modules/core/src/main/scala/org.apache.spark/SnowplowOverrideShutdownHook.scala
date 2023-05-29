/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package org.apache.spark

import cats.implicits._
import cats.effect.{Async, Deferred, Sync}
import cats.effect.std.Dispatcher
import cats.effect.kernel.Resource
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import org.apache.spark.util.{ShutdownHookManager => SparkShutdownHookManager}
import org.apache.hadoop.util.{ShutdownHookManager => HadoopShutdownHookManager}
import java.util.concurrent.TimeUnit

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
      _ <- Resource.eval(addSparkShutdownHook(dispatcher, sig))
      _ <- Resource.eval(addHadoopShutdownHook(dispatcher, sig))
    } yield ()

  private def addSparkShutdownHook[F[_]: Sync](dispatcher: Dispatcher[F], sig: Deferred[F, Unit]): F[Unit] =
    Sync[F].delay {
      SparkShutdownHookManager.addShutdownHook(999) { () =>
        try
          dispatcher.unsafeRunSync {
            Logger[F].info("Interrupted Spark's shutdown hook") >>
              sig.get
          }
        catch {
          case _: IllegalStateException =>
            // Expected if the cats-effect dispatcher is already shut down
            ()
        }
      }
    }.void

  private def addHadoopShutdownHook[F[_]: Sync](dispatcher: Dispatcher[F], sig: Deferred[F, Unit]): F[Unit] =
    Sync[F].delay {
      val runnable = new Runnable {
        def run(): Unit =
          try
            dispatcher.unsafeRunSync {
              Logger[F].info("Interrupted Hadoop's shutdown hook") >>
                sig.get
            }
          catch {
            case _: IllegalStateException =>
              // Expected if the cats-effect dispatcher is already shut down
              ()
          }
      }
      HadoopShutdownHookManager.get().addShutdownHook(runnable, 999, Int.MaxValue, TimeUnit.SECONDS)
    }

}
