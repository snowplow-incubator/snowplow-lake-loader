/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.processing

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
  override def timestampValue(v: Instant): Instant                = v
  override def dateValue(v: LocalDate): LocalDate                 = v
  override def arrayValue(vs: List[Any]): List[Any]               = vs
  override def structValue(vs: List[Caster.NamedValue[Any]]): Row = Row.fromSeq(vs.map(_.value))

}
