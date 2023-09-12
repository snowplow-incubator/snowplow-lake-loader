/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.processing

import org.apache.spark.sql.types._

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.loaders.AtomicFields

private[processing] object SparkSchema {

  /**
   * Builds the Spark schema for building a data frame of a batch of events
   *
   * The returned schema includes atomic fields and non-atomic fields but not the load_tstamp column
   */
  def forBatch(entities: List[Field]): StructType =
    StructType(atomic ::: entities.map(asSparkField))

  /**
   * Ordered Fields corresponding to the output from Enrich
   *
   * Does not include fields added by the loader, e.g. `load_tstamp`
   *
   * @note
   *   this is a `val` not a `def` because we use it over and over again.
   */
  val atomic: List[StructField] = AtomicFields.static.map(asSparkField)

  /**
   * Ordered spark Fields corresponding to the output of this loader
   *
   * Includes fields added by the loader, e.g. `load_tstamp`
   */
  def atomicPlusTimestamps: List[StructField] =
    atomic :+ StructField("load_tstamp", TimestampType, nullable = false)

  /** String representation of the atomic schema for creating a table using SQL dialiect */
  def ddlForTableCreate: String = StructType(atomicPlusTimestamps).toDDL

  private def asSparkField(ddlField: Field): StructField = {
    val normalizedName = Field.normalize(ddlField).name
    val dataType       = fieldType(ddlField.fieldType)
    StructField(normalizedName, dataType, ddlField.nullability.nullable)
  }

  private def fieldType(ddlType: Type): DataType = ddlType match {
    case Type.String                        => StringType
    case Type.Boolean                       => BooleanType
    case Type.Integer                       => IntegerType
    case Type.Long                          => LongType
    case Type.Double                        => DoubleType
    case Type.Decimal(precision, scale)     => DecimalType(Type.DecimalPrecision.toInt(precision), scale)
    case Type.Date                          => DateType
    case Type.Timestamp                     => TimestampType
    case Type.Struct(fields)                => StructType(fields.map(asSparkField))
    case Type.Array(element, elNullability) => ArrayType(fieldType(element), elNullability.nullable)
    case Type.Json                          => StringType // Spark does not support the `Json` parquet logical type.
  }
}
