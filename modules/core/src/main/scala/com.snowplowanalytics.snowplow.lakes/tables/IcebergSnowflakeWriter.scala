/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.tables

import com.snowplowanalytics.snowplow.lakes.Config

class IcebergSnowflakeWriter(config: Config.IcebergSnowflake) extends IcebergWriter(config) {

  override def sparkConfig: Map[String, String] =
    Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      s"spark.sql.catalog.$sparkCatalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$sparkCatalog.catalog-impl" -> "org.apache.iceberg.snowflake.SnowflakeCatalog",
      s"spark.sql.catalog.$sparkCatalog.uri" -> s"jdbc:snowflake://${config.host}",
      s"spark.sql.catalog.$sparkCatalog.jdbc.user" -> config.user,
      s"spark.sql.catalog.$sparkCatalog.jdbc.password" -> config.password,
      s"spark.sql.catalog.$sparkCatalog.jdbc.role" -> config.role.orNull

      // The "application" property is sadly not configurable because SnowflakeCatalog overrides it :(
      // s"spark.sql.catalog.$sparkCatalog.jdbc.application" -> "snowplow"
    )
}
