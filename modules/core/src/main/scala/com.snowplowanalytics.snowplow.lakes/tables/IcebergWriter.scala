/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.tables

import cats.implicits._
import cats.effect.Sync
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.processing.SparkSchema

/**
 * A base [[Writer]] for all flavours of Iceberg. Different concrete classes support different types
 * of catalog
 */
abstract class IcebergWriter(config: Config.Iceberg) extends Writer {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  // The name is not important, outside of this app
  final val sparkCatalog: String = "iceberg_catalog"

  override def prepareTable[F[_]: Sync](spark: SparkSession): F[Unit] =
    Logger[F].info(s"Creating Iceberg table $fqTable if it does not already exist...") >>
      Sync[F].blocking {
        spark.sql(s"""
          CREATE TABLE IF NOT EXISTS $fqTable
          (${SparkSchema.ddlForCreate})
          USING ICEBERG
          PARTITIONED BY (date(load_tstamp), event_name)
          TBLPROPERTIES('write.spark.accept-any-schema'='true')
          $locationClause
        """)
      }.void

  override def write[F[_]: Sync](df: DataFrame): F[Unit] =
    Sync[F].blocking {
      df.write
        .format("iceberg")
        .mode("append")
        .option("merge-schema", true)
        .option("check-ordering", false)
        .saveAsTable(fqTable)
    }

  // Fully qualified table name
  private def fqTable: String =
    s"$sparkCatalog.${config.database}.${config.table}"

  private def locationClause: String =
    config match {
      case _: Config.IcebergHadoop =>
        // Hadoop catalog does not allow overriding path-based location
        ""
      case _ =>
        s"LOCATION '${config.location}'"
    }

}
