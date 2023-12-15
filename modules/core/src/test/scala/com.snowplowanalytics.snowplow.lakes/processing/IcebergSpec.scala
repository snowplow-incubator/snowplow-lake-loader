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

class IcebergSpec extends AbstractSparkSpec {

  override def target: TestConfig.Target = TestConfig.Iceberg

  /** Reads the table back into memory, so we can make assertions on the app's output */
  override def readTable(spark: SparkSession, tmpDir: Path): DataFrame =
    spark.sql("select * from test_catalog.test.events")

  /** Spark config used only while reading table back into memory for assertions */
  override def sparkConfig(tmpDir: Path): Map[String, String] =
    Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.test_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.test_catalog.type" -> "hadoop",
      "spark.sql.catalog.test_catalog.warehouse" -> tmpDir.toString
    )
}
