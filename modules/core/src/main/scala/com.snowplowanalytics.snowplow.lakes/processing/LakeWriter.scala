/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.processing

import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.kernel.Resource
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import com.snowplowanalytics.snowplow.lakes.Config

trait LakeWriter[F[_]] {

  def createTable: F[Unit]

  def saveDataFrameToDisk(rows: NonEmptyList[Row], schema: StructType): F[DataFrameOnDisk]

  def removeDataFramesFromDisk(dataFramesOnDisk: List[DataFrameOnDisk]): F[Unit]

  def commit(dataFramesOnDisk: NonEmptyList[DataFrameOnDisk], nonAtomicFieldNames: Set[String]): F[Unit]
}

object LakeWriter {

  def build[F[_]: Async](config: Config.Spark, target: Config.Target): Resource[F, LakeWriter[F]] =
    for {
      spark <- SparkUtils.session[F](config, target)
    } yield new LakeWriter[F] {
      def createTable: F[Unit] =
        SparkUtils.createTable(spark, target)

      def saveDataFrameToDisk(rows: NonEmptyList[Row], schema: StructType): F[DataFrameOnDisk] =
        SparkUtils.saveDataFrameToDisk(spark, rows, schema)

      def removeDataFramesFromDisk(dataFramesOnDisk: List[DataFrameOnDisk]) =
        SparkUtils.dropViews(spark, dataFramesOnDisk)

      def commit(dataFramesOnDisk: NonEmptyList[DataFrameOnDisk], nonAtomicFieldNames: Set[String]): F[Unit] =
        SparkUtils.commit(spark, target, dataFramesOnDisk, nonAtomicFieldNames)
    }
}
