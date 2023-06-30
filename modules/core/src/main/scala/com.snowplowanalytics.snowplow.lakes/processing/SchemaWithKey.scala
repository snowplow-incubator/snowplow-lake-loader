/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.processing

import cats.Order

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.core.SchemaKey

private[processing] final case class SchemaWithKey(schemaKey: SchemaKey, schema: Schema)

private[processing] object SchemaWithKey {
  implicit def catsOrder: Order[SchemaWithKey] = Order.fromOrdering(Ordering.by(_.schemaKey))
}
