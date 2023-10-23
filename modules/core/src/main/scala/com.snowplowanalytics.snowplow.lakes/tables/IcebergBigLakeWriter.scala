/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.tables

import com.snowplowanalytics.snowplow.lakes.Config

class IcebergBigLakeWriter(config: Config.IcebergBigLake) extends IcebergWriter(config) {

  override def sparkConfig: Map[String, String] =
    Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      s"spark.sql.catalog.$sparkCatalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$sparkCatalog.catalog-impl" -> "org.apache.iceberg.gcp.biglake.BigLakeCatalog",
      s"spark.sql.catalog.$sparkCatalog.gcp_project" -> config.project,
      s"spark.sql.catalog.$sparkCatalog.gcp_location" -> config.region,
      s"spark.sql.catalog.$sparkCatalog.blms_catalog" -> config.catalog,
      s"spark.sql.catalog.$sparkCatalog.warehouse" -> config.location.toString
    )

  override def extraTableProperties: Map[String, String] =
    Map(
      "bq_table" -> s"${config.bqDataset}.${config.table}",
      "bq_connection" -> s"projects/${config.project}/locations/${config.region}/connections/${config.connection}"
    )
}
