/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.processing

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.snowplowanalytics.snowplow.lakes.TestConfig

import fs2.io.file.Path

class DeltaSpec extends AbstractSparkSpec {

  override def target: TestConfig.Target = TestConfig.Delta

  /** Reads the table back into memory, so we can make assertions on the app's output */
  override def readTable(spark: SparkSession, tmpDir: Path): DataFrame = {
    val location = (tmpDir / "events").toString
    spark.sql(s"""
      CREATE TABLE events USING delta
      LOCATION '$location'
    """)
    spark.sql("select * from events")
  }

  /** Spark config used only while reading table back into memory for assertions */
  override def sparkConfig(tmpDir: Path): Map[String, String] =
    Map(
      "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
}
