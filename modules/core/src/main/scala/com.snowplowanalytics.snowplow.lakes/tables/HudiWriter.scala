/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.tables

import cats.implicits._
import cats.effect.Sync
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.processing.SparkSchema

class HudiWriter(config: Config.Hudi) extends Writer {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  override def sparkConfig: Map[String, String] =
    Map(
      "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
    )

  override def prepareTable[F[_]: Sync](spark: SparkSession): F[Unit] = {
    val tableProps = config.hudiTableOptions
      .map { case (k, v) =>
        s"'$k'='$v'"
      }
      .mkString(", ")

    val internal_table_name = config.hudiTableOptions.get("hoodie.table.name").getOrElse("events")

    Logger[F].info(s"Creating Hudi table ${config.location} if it does not already exist...") >>
      Sync[F].blocking {
        spark.sql(s"""
          CREATE TABLE IF NOT EXISTS $internal_table_name
          (${SparkSchema.ddlForCreate})
          USING HUDI
          LOCATION '${config.location}'
          TBLPROPERTIES($tableProps)
        """)
      }.void
  }

  override def write[F[_]: Sync](df: DataFrame): F[Unit] =
    Sync[F].blocking {
      df.write
        .format("hudi")
        .mode("append")
        .options(config.hudiWriteOptions)
        .save(config.location.toString)
    }
}
