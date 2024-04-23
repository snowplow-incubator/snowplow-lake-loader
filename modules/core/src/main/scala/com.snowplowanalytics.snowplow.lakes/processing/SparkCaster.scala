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

import cats.data.NonEmptyVector
import io.circe.Json

import org.apache.spark.sql.Row

import java.time.{Instant, LocalDate}

import com.snowplowanalytics.iglu.schemaddl.parquet.Type
import com.snowplowanalytics.iglu.schemaddl.parquet.Caster

private[processing] object SparkCaster extends Caster[Any] {

  override def nullValue: AnyRef                 = null
  override def jsonValue(v: Json): String        = v.noSpaces
  override def stringValue(v: String): String    = v
  override def booleanValue(v: Boolean): Boolean = v
  override def intValue(v: Int): Int             = v
  override def longValue(v: Long): Long          = v
  override def doubleValue(v: Double): Double    = v
  override def decimalValue(unscaled: BigInt, details: Type.Decimal): BigDecimal =
    BigDecimal(unscaled, details.scale)
  override def timestampValue(v: Instant): Instant                          = v
  override def dateValue(v: LocalDate): LocalDate                           = v
  override def arrayValue(vs: Vector[Any]): Vector[Any]                     = vs
  override def structValue(vs: NonEmptyVector[Caster.NamedValue[Any]]): Row = row(vs.toVector)

  def row(vs: Vector[Caster.NamedValue[Any]]): Row = Row.fromSeq(vs.map(_.value))

}
