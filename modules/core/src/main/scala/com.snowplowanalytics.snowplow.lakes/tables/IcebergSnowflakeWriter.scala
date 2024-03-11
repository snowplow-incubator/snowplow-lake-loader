/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
