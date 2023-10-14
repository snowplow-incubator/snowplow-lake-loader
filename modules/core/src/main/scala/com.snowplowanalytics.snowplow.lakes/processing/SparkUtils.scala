/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.processing

import cats.data.NonEmptyList
import cats.effect.{Async, Sync}
import cats.effect.kernel.Resource
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.SnowplowOverrideShutdownHook
import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaConcurrentModificationException}
import io.delta.tables.DeltaTable

import com.snowplowanalytics.snowplow.lakes.Config

import java.util.UUID
import scala.jdk.CollectionConverters._

private[processing] object SparkUtils {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def session[F[_]: Async](
    config: Config.Spark,
    target: Config.Target
  ): Resource[F, SparkSession] = {
    val builder =
      SparkSession
        .builder()
        .appName("snowplow-lake-loader")
        .master(s"local[*, ${config.taskRetries}]")

    configureSparkForTarget(builder, target)
    configureSparkWithExtras(builder, config.conf)

    val openLogF  = Logger[F].info("Creating the global spark session...")
    val closeLogF = Logger[F].info("Closing the global spark session...")
    val buildF    = Sync[F].delay(builder.getOrCreate())

    Resource
      .make(openLogF >> buildF)(s => closeLogF >> Sync[F].blocking(s.close())) <*
      SnowplowOverrideShutdownHook.resource[F]
  }

  private def configureSparkForTarget(builder: SparkSession.Builder, target: Config.Target): Unit =
    target match {
      case Config.Delta(_, _) =>
        builder
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"): Unit
      case snowflake: Config.IcebergSnowflake =>
        builder
          .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
          .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
          .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.snowflake.SnowflakeCatalog")
          .config("spark.sql.catalog.iceberg_catalog.uri", s"jdbc:snowflake://${snowflake.host}")
          .config("spark.sql.catalog.iceberg_catalog.jdbc.user", snowflake.user)
          .config("spark.sql.catalog.iceberg_catalog.jdbc.password", snowflake.password)
          .config("spark.sql.catalog.iceberg_catalog.jdbc.role", snowflake.role.orNull): Unit
      // The "application" property is sadly not configurable because SnowflakeCatalog overrides it :(
      // .config("spark.sql.catalog.spark_catalog.jdbc.application", "snowplow")

      case biglake: Config.IcebergBigLake =>
        builder
          .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
          .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
          .config("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.gcp.biglake.BigLakeCatalog")
          .config("spark.sql.catalog.iceberg_catalog.gcp_project", biglake.project)
          .config("spark.sql.catalog.iceberg_catalog.gcp_location", biglake.region)
          .config("spark.sql.catalog.iceberg_catalog.blms_catalog", biglake.catalog)
          .config("spark.sql.catalog.iceberg_catalog.warehouse", biglake.location.toString): Unit
    }

  private def configureSparkWithExtras(builder: SparkSession.Builder, conf: Map[String, String]): Unit =
    conf.foreach { case (k, v) =>
      builder.config(k, v)
    }

  def createTable[F[_]: Sync](spark: SparkSession, target: Config.Target): F[Unit] =
    target match {
      case delta: Config.Delta     => createDelta(spark, delta)
      case iceberg: Config.Iceberg => createIceberg(spark, iceberg)
    }

  private def createDelta[F[_]: Sync](spark: SparkSession, target: Config.Delta): F[Unit] = {
    val builder = DeltaTable
      .createIfNotExists(spark)
      .partitionedBy("load_tstamp_date", "event_name")
      .location(target.location.toString)
      .tableName("events_internal_id") // The name does not matter
      .property("delta.dataSkippingNumIndexedCols", target.dataSkippingColumns.toSet.size.toString)

    SparkSchema.fieldsForDeltaCreate(target).foreach(builder.addColumn(_))

    // This column needs special treatment because of the `generatedAlwaysAs` clause
    builder.addColumn {
      DeltaTable
        .columnBuilder(spark, "load_tstamp_date")
        .dataType("DATE")
        .generatedAlwaysAs("CAST(load_tstamp AS DATE)")
        .nullable(false)
        .build()
    }: Unit

    Logger[F].info(s"Creating Delta table ${target.location} if it does not already exist...") >>
      Sync[F]
        .blocking(builder.execute())
        .void
        .recoverWith {
          case e: DeltaAnalysisException if e.errorClass === Some("DELTA_CREATE_TABLE_SCHEME_MISMATCH") =>
            // Expected when table exists and contains some unstruct_event or context columns
            Logger[F].debug(s"Caught and ignored DeltaAnalysisException")
        }
  }

  private def createIceberg[F[_]: Sync](spark: SparkSession, target: Config.Iceberg): F[Unit] = {
    val name = qualifiedNameForIceberg(target)
    val db   = qualifiedDbForIceberg(target)

    val extraProperties = target match {
      case biglake: Config.IcebergBigLake =>
        List(
          s"bq_table='${biglake.bqDataset}.${biglake.table}'",
          s"bq_connection='projects/${biglake.project}/locations/${biglake.region}/connections/${biglake.connection}'"
        )
      case _: Config.IcebergSnowflake =>
        Nil
    }

    val tblProperties = ("""'write.spark.accept-any-schema'='true'""" :: extraProperties).mkString(", ")

    Logger[F].info(s"Creating Iceberg table $name if it does not already exist...") >>
      Sync[F].blocking {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog"): Unit
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"): Unit
        spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $name
      (${SparkSchema.ddlForIcebergCreate})
      USING ICEBERG
      PARTITIONED BY (date(load_tstamp), event_name)
      TBLPROPERTIES($tblProperties)
      """)
      }.void
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
    target: Config.Target,
    dataFramesOnDisk: NonEmptyList[DataFrameOnDisk]
  ): F[Unit] = {
    val df = dataFramesOnDisk.toList
      .map(onDisk => spark.table(onDisk.viewName))
      .reduce(_.unionByName(_, allowMissingColumns = true))
      .coalesce(1)
      .withColumn("load_tstamp", current_timestamp())

    sinkForTarget(target, df)
  }

  private def sinkForTarget[F[_]: Sync](target: Config.Target, df: DataFrame): F[Unit] =
    target match {
      case delta: Config.Delta =>
        sinkDelta(delta, df)
      case iceberg: Config.Iceberg =>
        Sync[F].blocking {
          df.write
            .format("iceberg")
            .mode("append")
            .option("merge-schema", true)
            .option("check-ordering", false)
            .saveAsTable(qualifiedNameForIceberg(iceberg))
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
  private def sinkDelta[F[_]: Sync](target: Config.Delta, df: DataFrame): F[Unit] =
    Sync[F].untilDefinedM {
      Sync[F]
        .blocking[Option[Unit]] {
          df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", true)
            .save(target.location.toString)
          Some(())
        }
        .recoverWith { case e: DeltaConcurrentModificationException =>
          Logger[F]
            .warn(s"Retryable error writing to delta table: DeltaConcurrentModificationException with conflict type ${e.conflictType}")
            .as(None)
        }
    }

  def dropViews[F[_]: Sync](spark: SparkSession, dataFramesOnDisk: List[DataFrameOnDisk]): F[Unit] =
    Logger[F].info(s"Removing ${dataFramesOnDisk.size} spark data frames from local disk...") >>
      Sync[F].blocking {
        dataFramesOnDisk.foreach { onDisk =>
          spark.catalog.dropTempView(onDisk.viewName)
        }
      }

  private def qualifiedNameForIceberg(target: Config.Iceberg): String =
    target match {
      case sf: Config.IcebergSnowflake =>
        s"iceberg_catalog.${sf.schema}.${sf.table}"
      case bl: Config.IcebergBigLake =>
        s"iceberg_catalog.${bl.database}.${bl.table}"
    }

  private def qualifiedDbForIceberg(target: Config.Iceberg): String =
    target match {
      case sf: Config.IcebergSnowflake =>
        s"iceberg_catalog.${sf.schema}"
      case bl: Config.IcebergBigLake =>
        s"iceberg_catalog.${bl.database}"
    }

}
