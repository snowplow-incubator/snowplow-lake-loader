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
import cats.effect.{Async, Resource, Sync}
import cats.effect.std.Mutex
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}
import com.snowplowanalytics.snowplow.lakes.{Alert, Config, DestinationSetupErrorCheck, RuntimeService}
import com.snowplowanalytics.snowplow.lakes.tables.{DeltaWriter, HudiWriter, IcebergWriter, Writer}

trait LakeWriter[F[_]] {

  def createTable: F[Unit]

  /**
   * Creates an empty DataFrame with the atomic schema. Saves it with spark as a "view" so we can
   * refer to it by name later.
   *
   * Each window of events has its own DataFrame with unique name.
   */
  def initializeLocalDataFrame(viewName: String): F[Unit]

  /**
   * Append rows to the local DataFrame we are accumulating for this window
   *
   * This is a lazy operation: The resulting union-ed DataFrame is not evaluated until the end of
   * the window when we commit to the lake.
   *
   * @param viewName
   *   Spark view for this window. The view should already be initialized before calling
   *   `localAppendRows`.
   * @param rows
   *   The new rows to append
   * @param schema
   *   The schema for this batch of rows. This might include new entities that are not already in
   *   the existing DataFrame.
   */
  def localAppendRows(
    viewName: String,
    rows: NonEmptyList[Row],
    schema: StructType
  ): F[Unit]

  /**
   * Un-saves the Spark view
   *
   * This allows Spark to clean up space when we are finished using a DataFrame. This must be called
   * at the end of each window.
   */
  def removeDataFrameFromDisk(viewName: String): F[Unit]

  /** Commit the DataFrame by writing it into the lake */
  def commit(viewName: String): F[Unit]
}

object LakeWriter {

  trait WithHandledErrors[F[_]] extends LakeWriter[F]

  def build[F[_]: Async](
    config: Config.Spark,
    target: Config.Target
  ): Resource[F, LakeWriter[F]] = {
    val w = target match {
      case c: Config.Delta   => new DeltaWriter(c)
      case c: Config.Hudi    => new HudiWriter(c)
      case c: Config.Iceberg => new IcebergWriter(c)
    }
    for {
      session <- SparkUtils.session[F](config, w)
      writerParallelism = chooseWriterParallelism(config)
      mutex1 <- Resource.eval(Mutex[F])
      mutex2 <- Resource.eval(Mutex[F])
    } yield impl(session, w, writerParallelism, mutex1, mutex2)
  }

  def withHandledErrors[F[_]: Async](
    underlying: LakeWriter[F],
    appHealth: AppHealth.Interface[F, Alert, RuntimeService],
    retries: Config.Retries,
    destinationSetupErrorCheck: DestinationSetupErrorCheck
  ): WithHandledErrors[F] = new WithHandledErrors[F] {
    def createTable: F[Unit] =
      Retrying.withRetries(
        appHealth,
        retries.transientErrors,
        retries.setupErrors,
        RuntimeService.SparkWriter,
        Alert.FailedToCreateEventsTable,
        destinationSetupErrorCheck
      ) {
        underlying.createTable
      } <* appHealth.beHealthyForSetup

    def initializeLocalDataFrame(viewName: String): F[Unit] =
      underlying.initializeLocalDataFrame(viewName)

    def localAppendRows(
      viewName: String,
      rows: NonEmptyList[Row],
      schema: StructType
    ): F[Unit] =
      underlying.localAppendRows(viewName, rows, schema)

    def removeDataFrameFromDisk(viewName: String): F[Unit] =
      underlying.removeDataFrameFromDisk(viewName)

    def commit(viewName: String): F[Unit] =
      underlying
        .commit(viewName)
        .onError { case _ =>
          appHealth.beUnhealthyForRuntimeService(RuntimeService.SparkWriter)
        } <* appHealth.beHealthyForRuntimeService(RuntimeService.SparkWriter)
  }

  /**
   * Implementation of the LakeWriter
   *
   * The mutexes are needed because we allow overlapping windows. They prevent two different windows
   * from trying to run the same expensive operation at the same time.
   *
   * @param mutextForWriting
   *   Makes sure there is only ever one spark job trying to write events to the lake. This is a
   *   IO-intensive task.
   * @param mutexForUnioning
   *   Makes sure there is only ever one spark job trying to union smaller DataFrames into a larger
   *   DataFrame, immediately before writing to the lake. This is a cpu-intensive task.
   */
  private def impl[F[_]: Sync](
    spark: SparkSession,
    w: Writer,
    writerParallelism: Int,
    mutexForWriting: Mutex[F],
    mutexForUnioning: Mutex[F]
  ): LakeWriter[F] = new LakeWriter[F] {
    def createTable: F[Unit] =
      w.prepareTable(spark)

    def initializeLocalDataFrame(viewName: String): F[Unit] =
      SparkUtils.initializeLocalDataFrame(spark, viewName)

    def localAppendRows(
      viewName: String,
      rows: NonEmptyList[Row],
      schema: StructType
    ): F[Unit] =
      SparkUtils.localAppendRows(spark, viewName, rows, schema)

    def removeDataFrameFromDisk(viewName: String) =
      SparkUtils.dropView(spark, viewName)

    def commit(viewName: String): F[Unit] =
      for {
        df <- mutexForUnioning.lock.surround {
                SparkUtils.prepareFinalDataFrame(spark, viewName, writerParallelism)
              }
        _ <- mutexForWriting.lock
               .surround {
                 w.write(df)
               }
      } yield ()
  }

  /**
   * Converts `writerParallelismFraction` into a suggested number of threads
   *
   * For bigger instances (more cores) we want more parallelism in the writer. This avoids a
   * situation where writing tasks exceed the length of a window, which causes an unbalanced use of
   * cpu.
   */
  private def chooseWriterParallelism(config: Config.Spark): Int =
    (Runtime.getRuntime.availableProcessors * config.writerParallelismFraction)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt
}
