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

import org.apache.spark.sql.types._

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.loaders.transform.{AtomicFields, TypedTabledEntity}

object SparkSchema {

  /**
   * Builds the Spark schema for building a data frame of a batch of events
   *
   * The returned schema includes atomic fields and non-atomic fields but not the load_tstamp column
   */
  def forBatch(entities: Vector[TypedTabledEntity], respectIgluNullability: Boolean): StructType = {
    val nonAtomicFields = entities.flatMap { tte =>
      tte.mergedField :: tte.recoveries.map(_._2)
    }
    StructType(atomic ++ nonAtomicFields.map(asSparkField(_, respectIgluNullability)))
  }

  /**
   * Ordered Fields corresponding to the output from Enrich
   *
   * Does not include fields added by the loader, e.g. `load_tstamp`
   *
   * @note
   *   this is a `val` not a `def` because we use it over and over again.
   */
  val atomic: Vector[StructField] = AtomicFields.static.map(asSparkField(_, true))

  /** String representation of the atomic schema for creating a table using SQL dialiect */
  def ddlForCreate: String =
    StructType(AtomicFields.withLoadTstamp.map(asSparkField(_, true))).toDDL

  def asSparkField(ddlField: Field, respectIgluNullability: Boolean): StructField = {
    val normalizedName = Field.normalize(ddlField).name
    val dataType       = fieldType(ddlField.fieldType, respectIgluNullability)
    StructField(normalizedName, dataType, !respectIgluNullability || ddlField.nullability.nullable)
  }

  private def fieldType(ddlType: Type, respectIgluNullability: Boolean): DataType = ddlType match {
    case Type.String                    => StringType
    case Type.Boolean                   => BooleanType
    case Type.Integer                   => IntegerType
    case Type.Long                      => LongType
    case Type.Double                    => DoubleType
    case Type.Decimal(precision, scale) => DecimalType(Type.DecimalPrecision.toInt(precision), scale)
    case Type.Date                      => DateType
    case Type.Timestamp                 => TimestampType
    case Type.Struct(fields)            => StructType(fields.toVector.map(asSparkField(_, respectIgluNullability)))
    case Type.Array(element, elNullability) =>
      ArrayType(fieldType(element, respectIgluNullability), !respectIgluNullability || elNullability.nullable)
    case Type.Json => StringType // Spark does not support the `Json` parquet logical type.
  }
}
