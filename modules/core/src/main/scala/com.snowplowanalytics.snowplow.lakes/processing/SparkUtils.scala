/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.tables.Writer
import com.snowplowanalytics.snowplow.lakes.fs.LakeLoaderFileSystem

import scala.jdk.CollectionConverters._
import java.net.URI
import java.time.Instant

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
        if (writer.toleratesAsyncDelete) {
          Sync[F].delay {
            // Forces Spark to use `LakeLoaderFileSystem` when writing to the Lake via Hadoop
            LakeLoaderFileSystem.overrideHadoopFileSystemConf(targetLocation, session.sparkContext.hadoopConfiguration)
          }
        } else Sync[F].unit
      }
  }

  private def sparkConfigOptions(config: Config.Spark, writer: Writer): Map[String, String] = {
    val gcpUserAgentKey   = "fs.gs.storage.http.headers.user-agent"
    val gcpUserAgentValue = s"${config.gcpUserAgent.productName}/lake-loader (GPN:Snowplow;)"
    writer.sparkConfig ++ config.conf + (gcpUserAgentKey -> gcpUserAgentValue)
  }

  def initializeLocalDataFrame[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    rows: List[Row],
    schema: StructType
  ): F[Unit] =
    for {
      _ <- Logger[F].debug(s"Initializing local DataFrame with name $viewName and ${rows.size} events")
      _ <- Sync[F].blocking {
             spark
               .createDataFrame(rows.toList.asJava, schema)
               .coalesce(1)
               .localCheckpoint()
               .createTempView(viewName)
           }
    } yield ()

  def localAppendRows[F[_]: Sync](
    spark: SparkSession,
    viewName: String,
    rows: List[Row],
    schema: StructType
  ): F[Unit] =
    for {
      _ <- Logger[F].debug(s"Saving batch of ${rows.size} events to local DataFrame $viewName")
      _ <- Sync[F].blocking {
             spark
               .createDataFrame(rows.asJava, schema)
               .coalesce(1)
               .localCheckpoint()
               .unionByName(spark.table(viewName), allowMissingColumns = true)
               .createOrReplaceTempView(viewName)
           }
    } yield ()

  def prepareFinalDataFrame[F[_]: Sync](
    spark: SparkSession,
    viewNames: NonEmptyList[String],
    writerParallelism: Int,
    loadTstamp: Instant
  ): F[DataFrame] =
    Sync[F].blocking {
      viewNames
        .map { viewName =>
          spark
            .table(viewName)
            .withColumn("load_tstamp", lit(loadTstamp))
            .coalesce(1)
        }
        .reduceLeft(_.unionByName(_, allowMissingColumns = true))
        .coalesce(writerParallelism)
        .localCheckpoint()
    }

  def dropView[F[_]: Sync](spark: SparkSession, viewName: String): F[Unit] =
    Logger[F].debug(s"Removing Spark data frame $viewName from local disk...") >>
      Sync[F].blocking {
        spark.catalog.dropTempView(viewName)
      }.void
}
