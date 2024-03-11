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

}
