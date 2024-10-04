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

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.snowplowanalytics.snowplow.lakes.TestConfig

import fs2.io.file.Path

class HudiSpec extends AbstractSparkSpec {

  // TODO: After Hudi 1.0.0 is released, remove `skipAll` to re-enable these tests
  override def is = skipAll ^ super.is

  override def target: TestConfig.Target = TestConfig.Hudi

  /** Reads the table back into memory, so we can make assertions on the app's output */
  override def readTable(spark: SparkSession, tmpDir: Path): DataFrame = {
    val location = (tmpDir / "events").toString
    spark.sql(s"""
      CREATE TABLE events USING hudi
      LOCATION '$location'
    """): Unit
    spark.sql("select * from events")
  }

  /** Spark config used only while reading table back into memory for assertions */
  override def sparkConfig(tmpDir: Path): Map[String, String] =
    Map(
      "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
    )

}
