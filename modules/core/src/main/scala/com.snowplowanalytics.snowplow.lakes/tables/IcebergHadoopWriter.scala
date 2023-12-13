/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.tables

import com.snowplowanalytics.snowplow.lakes.Config

class IcebergHadoopWriter(config: Config.IcebergHadoop) extends IcebergWriter.WithDefaults(config) {
  override def sparkConfig: Map[String, String] =
    Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      s"spark.sql.catalog.$sparkCatalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$sparkCatalog.type" -> "hadoop",
      s"spark.sql.catalog.$sparkCatalog.warehouse" -> config.location.toString
    )
}
