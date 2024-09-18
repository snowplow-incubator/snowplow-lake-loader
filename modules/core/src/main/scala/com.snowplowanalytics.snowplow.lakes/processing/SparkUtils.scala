/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.processing

import cats.data.NonEmptyList
import cats.effect.{Async, Sync}
import cats.effect.kernel.Resource
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.types.StructType

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.tables.Writer
import com.snowplowanalytics.snowplow.lakes.fs.LakeLoaderFileSystem

import scala.jdk.CollectionConverters._
import java.net.URI

private[processing] object SparkUtils {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def session[F[_]: Async](
    config: Config.Spark,
    writer: Writer,
    targetLocation: URI
  ): Resource[F, SparkSession] = {
    val builder =
      SparkSession
        .builder()
        .appName("snowplow-lake-loader")
        .master(s"local[*, ${config.taskRetries}]")
        .config(sparkConfigOptions(config, writer))

    val openLogF  = Logger[F].info("Creating the global spark session...")
    val closeLogF = Logger[F].info("Closing the global spark session...")
    val buildF    = Sync[F].delay(builder.getOrCreate())

    Resource
      .make(openLogF >> buildF)(s => closeLogF >> Sync[F].blocking(s.close()))
      .evalTap { session =>
        setHadoopFileSystem[F](session.sparkContext.hadoopConfiguration, targetLocation)
      }
  }

  private def setHadoopFileSystem[F[_]: Sync](conf: HadoopConfiguration, targetLocation: URI): F[Unit] = Sync[F].delay {
    val scheme = targetLocation.getScheme
    Option(conf.get(s"fs.$scheme.impl")).foreach { previousImpl =>
      conf.set(s"fs.$scheme.lakeloader.delegate.impl", previousImpl)
    }
    conf.set(s"fs.$scheme.impl", classOf[LakeLoaderFileSystem].getName)
  }

  private def sparkConfigOptions(config: Config.Spark, writer: Writer): Map[String, String] = {
    val gcpUserAgentKey   = "fs.gs.storage.http.headers.user-agent"
    val gcpUserAgentValue = s"${config.gcpUserAgent.productName}/lake-loader (GPN:Snowplow;)"
    writer.sparkConfig ++ config.conf + (gcpUserAgentKey -> gcpUserAgentValue)
  }

  def initializeLocalDataFrame[F[_]: Sync](spark: SparkSession, viewName: String): F[Unit] =
    for {
      _ <- Logger[F].debug(s"Initializing local DataFrame with name $viewName")
      _ <- Sync[F].blocking {
             spark.emptyDataFrame.createTempView(viewName)
           }
    } yield ()

  def localAppendRows[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    rows: NonEmptyList[Row],
    schema: StructType
  ): F[Unit] =
    for {
      _ <- Logger[F].debug(s"Saving batch of ${rows.size} events to local DataFrame $viewName")
      _ <- Sync[F].blocking {
             spark
               .createDataFrame(rows.toList.asJava, schema)
               .coalesce(1)
               .localCheckpoint()
               .unionByName(spark.table(viewName), allowMissingColumns = true)
               .createOrReplaceTempView(viewName)
           }
    } yield ()

  def prepareFinalDataFrame[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    writerParallelism: Int
  ): F[DataFrame] =
    Sync[F].blocking {
      spark
        .table(viewName)
        .withColumn("load_tstamp", current_timestamp())
        .coalesce(writerParallelism)
        .localCheckpoint()
    }

  def dropView[F[_]: Sync](spark: SparkSession, viewName: String): F[Unit] =
    Logger[F].info(s"Removing Spark data frame $viewName from local disk...") >>
      Sync[F].blocking {
        spark.catalog.dropTempView(viewName)
      }.void
}
