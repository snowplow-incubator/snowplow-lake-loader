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
import cats.effect.Async
import cats.effect.kernel.Resource
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.tables.{DeltaWriter, IcebergBigLakeWriter, IcebergSnowflakeWriter, Writer}

trait LakeWriter[F[_]] {

  def createTable: F[Unit]

  def saveDataFrameToDisk(rows: NonEmptyList[Row], schema: StructType): F[DataFrameOnDisk]

  def removeDataFramesFromDisk(dataFramesOnDisk: List[DataFrameOnDisk]): F[Unit]

  def commit(dataFramesOnDisk: NonEmptyList[DataFrameOnDisk]): F[Unit]
}

object LakeWriter {

  def build[F[_]: Async](
    config: Config.Spark,
    target: Config.Target
  ): Resource[F, LakeWriter[F]] = {
    val w = target match {
      case c: Config.Delta            => new DeltaWriter(c)
      case c: Config.IcebergBigLake   => new IcebergBigLakeWriter(c)
      case c: Config.IcebergSnowflake => new IcebergSnowflakeWriter(c)
    }
    SparkUtils.session[F](config, w).map(impl(_, w))
  }

  private def impl[F[_]: Async](
    spark: SparkSession,
    w: Writer
  ): LakeWriter[F] = new LakeWriter[F] {
    def createTable: F[Unit] =
      w.prepareTable(spark)

    def saveDataFrameToDisk(rows: NonEmptyList[Row], schema: StructType): F[DataFrameOnDisk] =
      SparkUtils.saveDataFrameToDisk(spark, rows, schema)

    def removeDataFramesFromDisk(dataFramesOnDisk: List[DataFrameOnDisk]) =
      SparkUtils.dropViews(spark, dataFramesOnDisk)

    def commit(dataFramesOnDisk: NonEmptyList[DataFrameOnDisk]): F[Unit] =
      SparkUtils.commit(spark, w, dataFramesOnDisk)
  }
}
