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
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaConcurrentModificationException}
import org.apache.spark.sql.types.StructField
import io.delta.tables.DeltaTable

import com.snowplowanalytics.snowplow.lakes.Config
import com.snowplowanalytics.snowplow.lakes.processing.SparkSchema
import com.snowplowanalytics.snowplow.loaders.transform.AtomicFields

class DeltaWriter(config: Config.Delta) extends Writer {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  override def sparkConfig: Map[String, String] =
    Map(
      "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

  override def prepareTable[F[_]: Sync](spark: SparkSession): F[Unit] = {
    val builder = DeltaTable
      .createIfNotExists(spark)
      .partitionedBy("load_tstamp_date", "event_name")
      .location(config.location.toString)
      .tableName("events_internal_id") // The name does not matter
      .property("delta.dataSkippingNumIndexedCols", config.dataSkippingColumns.toSet.size.toString())

    fieldsForCreate(config).foreach(builder.addColumn(_))

    // This column needs special treatment because of the `generatedAlwaysAs` clause
    builder.addColumn {
      DeltaTable
        .columnBuilder(spark, "load_tstamp_date")
        .dataType("DATE")
        .generatedAlwaysAs("CAST(load_tstamp AS DATE)")
        .nullable(false)
        .build()
    }

    Logger[F].info(s"Creating Delta table ${config.location} if it does not already exist...") >>
      Sync[F]
        .blocking(builder.execute())
        .void
        .recoverWith {
          case e: DeltaAnalysisException if e.errorClass === Some("DELTA_CREATE_TABLE_SCHEME_MISMATCH") =>
            // Expected when table exists and contains some unstruct_event or context columns
            Logger[F].debug(s"Caught and ignored DeltaAnalysisException")
        }
  }

  /**
   * Sink to delta with retries
   *
   * Retry is needed if a concurrent writer updated the table metadata. It is only needed during
   * schema evolution, when the pipeine starts tracking a new schema for the first time.
   *
   * Retry happens immediately with no delay. For this type of exception there is no reason to
   * delay.
   */
  override def write[F[_]: Sync](df: DataFrame): F[Unit] =
    Sync[F].untilDefinedM {
      Sync[F]
        .blocking[Option[Unit]] {
          df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", true)
            .save(config.location.toString)
          Some(())
        }
        .recoverWith { case e: DeltaConcurrentModificationException =>
          Logger[F]
            .warn(s"Retryable error writing to delta table: DeltaConcurrentModificationException with conflict type ${e.conflictType}")
            .as(None)
        }
    }

  /**
   * Ordered spark Fields corresponding to the output of this loader
   *
   * Includes fields added by the loader, e.g. `load_tstamp`
   *
   * @param config
   *   The Delta config, whose `dataSkippingColumn` param tells us which columns must go first in
   *   the table definition. See Delta's data skipping feature to understand why.
   */
  private def fieldsForCreate(config: Config.Delta): List[StructField] = {
    val (withStats, noStats) = AtomicFields.withLoadTstamp.partition { f =>
      config.dataSkippingColumns.contains(f.name)
    }
    (withStats ::: noStats).map(SparkSchema.asSparkField)
  }

}
