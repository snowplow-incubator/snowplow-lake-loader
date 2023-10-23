/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.tables

import cats.effect.Sync
import org.apache.spark.sql.{DataFrame, SparkSession}

/** The methods needed for a writing specific table format (e.g. delta, iceberg or hudi) */
trait Writer {

  /** Spark config parameters which the Lake Loader needs for this specific table format */
  def sparkConfig: Map[String, String]

  /**
   * Prepare a table to be ready for loading. Runs once when the app first starts up.
   *
   * For some table formats that can mean creating the table, or registering it in an external
   * catalog.
   */
  def prepareTable[F[_]: Sync](spark: SparkSession): F[Unit]

  /** Write Snowplow events into the table */
  def write[F[_]: Sync](df: DataFrame): F[Unit]
}
