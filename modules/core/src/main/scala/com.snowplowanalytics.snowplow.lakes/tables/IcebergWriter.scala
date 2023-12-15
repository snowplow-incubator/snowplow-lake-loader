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

  /** Abstract methods */

  def extraTableProperties: Map[String, String]

  def requiresCreateNamespace: Boolean

  /* End of abstract methods */

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  // The name is not important, outside of this app
  final val sparkCatalog: String = "iceberg_catalog"

  private val defaultTableProperties: Map[String, String] =
    Map(
      "write.spark.accept-any-schema" -> "true"
    )

  override def prepareTable[F[_]: Sync](spark: SparkSession): F[Unit] = {
    val tableProps = (defaultTableProperties ++ extraTableProperties)
      .map { case (k, v) =>
        s"'$k'='$v'"
      }
      .mkString(", ")

    Logger[F].info(s"Creating Iceberg table $fqTable if it does not already exist...") >>
      Sync[F].blocking {
        if (requiresCreateNamespace) {
          spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $sparkCatalog")
        }
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $fqDatabase")
        spark.sql(s"""
          CREATE TABLE IF NOT EXISTS $fqTable
          (${SparkSchema.ddlForCreate})
          USING ICEBERG
          PARTITIONED BY (date(load_tstamp), event_name)
          TBLPROPERTIES($tableProps)
        """)
      }.void
  }

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
    s"$sparkCatalog.${config.sparkDatabase}.${config.table}"

  // Fully qualified database name
  private def fqDatabase: String =
    s"$sparkCatalog.${config.sparkDatabase}"

}

object IcebergWriter {

  abstract class WithDefaults(config: Config.Iceberg) extends IcebergWriter(config: Config.Iceberg) {

    override def requiresCreateNamespace: Boolean = false

    override def extraTableProperties: Map[String, String] = Map.empty
  }
}
