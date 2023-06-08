package com.snowplowanalytics.snowplow.lakes.processing

import cats.Order

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.core.SchemaKey

private[processing] final case class SchemaWithKey(schemaKey: SchemaKey, schema: Schema)

private[processing] object SchemaWithKey {
  implicit def catsOrder: Order[SchemaWithKey] = Order.fromOrdering(Ordering.by(_.schemaKey))
}
