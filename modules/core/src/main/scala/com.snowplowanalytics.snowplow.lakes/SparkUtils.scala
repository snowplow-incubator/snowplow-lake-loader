package com.snowplowanalytics.snowplow.lakes

import cats.effect.Sync
import cats.effect.kernel.Resource
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.types.StructType
import io.delta.tables.DeltaTable

import java.util.UUID
import scala.jdk.CollectionConverters._

object SparkUtils {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  case class DataFrameOnDisk(viewName: String, count: Int)

  def session[F[_]: Sync](config: Config.Spark, target: Config.Target): Resource[F, SparkSession] = {
    val builder =
      SparkSession
        .builder()
        .appName("snowplow-lake-loader")
        .master(s"local[${config.threads}, ${config.retries}]")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", s"${config.targetParquetSizeMB}MB")
        .config("spark.local.dir", config.localDir)
        .config("spark.ui.enabled", false)

    configureSparkForTarget(builder, target)
    configureSparkWithExtras(builder, config.conf)

    val openLogF = Logger[F].info("Creating the global spark session...")
    val closeLogF = Logger[F].info("Closing the global spark session...")
    val buildF = Sync[F].delay(builder.getOrCreate())

    Resource.make(openLogF >> buildF)(s => closeLogF >> Sync[F].blocking(s.close()))
  }

  private def configureSparkForTarget(builder: SparkSession.Builder, target: Config.Target): Unit =
    target match {
      case Config.Delta(_) =>
        builder
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"): Unit
      case snowflake: Config.IcebergSnowflake =>
        builder
          .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
          .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
          .config("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.snowflake.SnowflakeCatalog")
          .config("spark.sql.catalog.spark_catalog.uri", s"jdbc:snowflake://${snowflake.host}")
          .config("spark.sql.catalog.spark_catalog.jdbc.user", snowflake.user)
          .config("spark.sql.catalog.spark_catalog.jdbc.password", snowflake.password)
          .config("spark.sql.catalog.spark_catalog.jdbc.role", snowflake.role.orNull): Unit

      // The "application" property is sadly not configurable because SnowflakeCatalog overrides it :(
      // .config("spark.sql.catalog.spark_catalog.jdbc.application", "snowplow")
    }

  private def configureSparkWithExtras(builder: SparkSession.Builder, conf: Map[String, String]): Unit =
    conf.foreach { case (k, v) =>
      builder.config(k, v)
    }

  def createTable[F[_]: Sync](spark: SparkSession, target: Config.Target): F[Unit] =
    target match {
      case delta: Config.Delta => createDelta(spark, delta)
      case iceberg: Config.Iceberg => createIceberg(spark, iceberg)
    }

  def createDelta[F[_]: Sync](spark: SparkSession, target: Config.Delta): F[Unit] = {
    val builder = DeltaTable
      .createIfNotExists(spark)
      .partitionedBy("load_tstamp_date", "event_name")
      .location(target.location.toString)
      .tableName("events_internal_id") // The name does not matter

    AtomicFields.sparkFieldsWithLoadTstamp.foreach(builder.addColumn(_))

    // This column needs special treatment because of the `generatedAlwaysAs` clause
    builder.addColumn {
      DeltaTable
        .columnBuilder("load_tstamp_date")
        .dataType("DATE")
        .generatedAlwaysAs("CAST(load_tstamp AS DATE)")
        .nullable(false)
        .build()
    }: Unit

    Logger[F].info(s"Creating Delta table ${target.location} if it does not already exist...") >>
      Sync[F].blocking(builder.execute()).void
  }

  def createIceberg[F[_]: Sync](spark: SparkSession, target: Config.Iceberg): F[Unit] = {
    val name = qualifiedNameForIceberg(target)
    Logger[F].info(s"Creating Iceberg table $name if it does not already exist...") >>
      Sync[F].blocking {
        spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $name
      (${AtomicFields.sparkDDL})
      USING ICEBERG
      PARTITIONED BY (date(load_tstamp), event_name)
      LOCATION ${target.location}
      TBLPROPERTIES('write.spark.accept-any-schema'='true')
      """)
      }.void
  }

  def saveDataFrameToDisk[F[_]: Sync](
    spark: SparkSession,
    rows: List[Row],
    schema: StructType
  ): F[Option[DataFrameOnDisk]] = {
    val count = rows.size
    if (count > 0) {
      for {
        viewName <- Sync[F].delay(UUID.randomUUID.toString.replaceAll("-", ""))
        _ <- Logger[F].info(s"Saving batch of $count events to local disk")
        _ <- Sync[F].blocking {
               spark
                 .createDataFrame(rows.asJava, schema)
                 .localCheckpoint()
                 .createTempView(viewName)
             }
      } yield Some(DataFrameOnDisk(viewName, count))
    } else {
      Logger[F].info(s"An in-memory batch yielded zero good events.  Nothing will be saved to local disk.").as(None)
    }
  }

  def sink[F[_]: Sync](
    spark: SparkSession,
    target: Config.Target,
    dataFramesOnDisk: List[DataFrameOnDisk],
    nonAtomicFieldNames: Set[String]
  ): F[Unit] = {
    val totalCount = dataFramesOnDisk.map(_.count).sum
    if (totalCount > 0) {
      val df = dataFramesOnDisk
        .map(onDisk => spark.table(onDisk.viewName))
        .reduce(_.unionByName(_, allowMissingColumns = true))
        .hint("rebalance")
        .withColumn("load_tstamp", current_timestamp())

      Logger[F].info(s"Ready to Write and commit $totalCount events to the lake.") >>
        Logger[F].info(s"Non atomic columns: [${nonAtomicFieldNames.toSeq.sorted.mkString(",")}]") >>
        sinkForTarget(target, df) >>
        Logger[F].info(s"Finished writing and committing $totalCount events to the lake.")
    } else {
      Logger[F].info("A window yielded zero good events.  Nothing will be written into the lake.")
    }
  }

  private def sinkForTarget[F[_]: Sync](target: Config.Target, df: DataFrame): F[Unit] =
    target match {
      case Config.Delta(location) =>
        Sync[F].blocking {
          df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", true)
            .save(location.toString)
        }
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
        s"${sf.database}.${sf.schema}.${sf.table}"
    }

}
