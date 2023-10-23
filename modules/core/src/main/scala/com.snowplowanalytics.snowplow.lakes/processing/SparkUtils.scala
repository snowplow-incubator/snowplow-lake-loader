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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.SnowplowOverrideShutdownHook

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.tables.Writer

import java.util.UUID
import scala.jdk.CollectionConverters._

private[processing] object SparkUtils {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def session[F[_]: Async](
    config: Config.Spark,
    writer: Writer
  ): Resource[F, SparkSession] = {
    val builder =
      SparkSession
        .builder()
        .appName("snowplow-lake-loader")
        .master(s"local[*, ${config.taskRetries}]")

    builder.config(writer.sparkConfig ++ config.conf)

    val openLogF  = Logger[F].info("Creating the global spark session...")
    val closeLogF = Logger[F].info("Closing the global spark session...")
    val buildF    = Sync[F].delay(builder.getOrCreate())

    Resource
      .make(openLogF >> buildF)(s => closeLogF >> Sync[F].blocking(s.close())) <*
      SnowplowOverrideShutdownHook.resource[F]
  }

  def saveDataFrameToDisk[F[_]: Sync](
    spark: SparkSession,
    rows: NonEmptyList[Row],
    schema: StructType
  ): F[DataFrameOnDisk] = {
    val count = rows.size
    for {
      viewName <- Sync[F].delay(UUID.randomUUID.toString.replaceAll("-", ""))
      _ <- Logger[F].debug(s"Saving batch of $count events to local disk")
      _ <- Sync[F].blocking {
             spark
               .createDataFrame(rows.toList.asJava, schema)
               .coalesce(1)
               .localCheckpoint() // REMOVE this line to use memory instead of disk
               .createTempView(viewName)
           }
    } yield DataFrameOnDisk(viewName, count)
  }

  def commit[F[_]: Sync](
    spark: SparkSession,
    writer: Writer,
    dataFramesOnDisk: NonEmptyList[DataFrameOnDisk]
  ): F[Unit] = {
    val df = dataFramesOnDisk.toList
      .map(onDisk => spark.table(onDisk.viewName))
      .reduce(_.unionByName(_, allowMissingColumns = true))
      .coalesce(1)
      .withColumn("load_tstamp", current_timestamp())

    writer.write(df)
  }

  def dropViews[F[_]: Sync](spark: SparkSession, dataFramesOnDisk: List[DataFrameOnDisk]): F[Unit] =
    Logger[F].info(s"Removing ${dataFramesOnDisk.size} spark data frames from local disk...") >>
      Sync[F].blocking {
        dataFramesOnDisk.foreach { onDisk =>
          spark.catalog.dropTempView(onDisk.viewName)
        }
      }
}
