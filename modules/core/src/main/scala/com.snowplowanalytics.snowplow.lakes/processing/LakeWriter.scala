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

import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.{Async, Ref, Resource}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.tables.{DeltaWriter, HudiWriter, IcebergBigLakeWriter, IcebergSnowflakeWriter, Writer}

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
  ): Resource[F, (LakeWriter[F], F[HealthProbe.Status])] = {
    val w = target match {
      case c: Config.Delta            => new DeltaWriter(c)
      case c: Config.Hudi             => new HudiWriter(c)
      case c: Config.IcebergBigLake   => new IcebergBigLakeWriter(c)
      case c: Config.IcebergSnowflake => new IcebergSnowflakeWriter(c)
    }
    for {
      session <- SparkUtils.session[F](config, w)
      isHealthy <- Resource.eval(Ref[F].of(initialHealthStatus))
    } yield (impl(session, w, isHealthy), isHealthy.get)
  }

  private def initialHealthStatus: HealthProbe.Status =
    HealthProbe.Unhealthy("Destination table not initialized yet")

  private def impl[F[_]: Async](
    spark: SparkSession,
    w: Writer,
    isHealthy: Ref[F, HealthProbe.Status]
  ): LakeWriter[F] = new LakeWriter[F] {
    def createTable: F[Unit] =
      w.prepareTable(spark) <* isHealthy.set(HealthProbe.Healthy)

    def saveDataFrameToDisk(rows: NonEmptyList[Row], schema: StructType): F[DataFrameOnDisk] =
      SparkUtils.saveDataFrameToDisk(spark, rows, schema)

    def removeDataFramesFromDisk(dataFramesOnDisk: List[DataFrameOnDisk]) =
      SparkUtils.dropViews(spark, dataFramesOnDisk)

    def commit(dataFramesOnDisk: NonEmptyList[DataFrameOnDisk]): F[Unit] =
      SparkUtils.commit(spark, w, dataFramesOnDisk)
  }
}
