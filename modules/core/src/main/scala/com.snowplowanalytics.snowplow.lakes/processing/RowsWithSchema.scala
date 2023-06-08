package com.snowplowanalytics.snowplow.lakes.processing

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

private[processing] final case class RowsWithSchema(rows: List[Row], schema: StructType)
