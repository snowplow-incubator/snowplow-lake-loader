/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package org.apache.spark.sql

import org.apache.spark.sql.types.StructType

// Intentionally placed in org.apache.spark.sql so that scalac permits the call to
// classic.SparkSession.internalCreateDataFrame, which is private[sql] (a qualifier enforced by
// scalac from the pickled Scala signature, even though the method is a public JVM member).
//
// reattachSchema passes the DataFrame's existing RDD[InternalRow] directly to
// internalCreateDataFrame, bypassing the InternalRow→Row→InternalRow roundtrip that
// the public createDataFrame(rdd: RDD[Row], schema) API incurs (nullability is
// metadata-only: it is never enforced at runtime).
//
// internalCreateDataFrame exists only on the classic (non-Connect) implementation of
// the Spark SQL API. The loader always runs a local classic session, so the cast is safe.
// Not part of the public API of this project.
object SnowplowInternalSparkBridge {
  def reattachSchema(df: DataFrame, schema: StructType): DataFrame = {
    val classicDf = df.asInstanceOf[classic.Dataset[Row]]
    classicDf.sparkSession.internalCreateDataFrame(classicDf.queryExecution.toRdd, schema)
  }
}
