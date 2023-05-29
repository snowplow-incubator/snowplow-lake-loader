package com.snowplowanalytics.snowplow.lakes

import org.apache.spark.sql.types._

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}

object SparkSchema {

  def build(entities: List[Field]): StructType =
    StructType(AtomicFields.sparkFields ::: entities.map(asSparkField))

  def asSparkField(ddlField: Field): StructField = {
    val normalizedName = Field.normalize(ddlField).name
    val dataType = fieldType(ddlField.fieldType)
    StructField(normalizedName, dataType, ddlField.nullability.nullable)
  }

  private def fieldType(ddlType: Type): DataType = ddlType match {
    case Type.String => StringType
    case Type.Boolean => BooleanType
    case Type.Integer => IntegerType
    case Type.Long => LongType
    case Type.Double => DoubleType
    case Type.Decimal(precision, scale) => DecimalType(Type.DecimalPrecision.toInt(precision), scale)
    case Type.Date => DateType
    case Type.Timestamp => TimestampType
    case Type.Struct(fields) => StructType(fields.map(asSparkField))
    case Type.Array(element, elNullability) => ArrayType(fieldType(element), elNullability.nullable)
    case Type.Json => StringType // Spark does not support the `Json` parquet logical type.
  }
}
