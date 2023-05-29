package com.snowplowanalytics.snowplow

import com.snowplowanalytics.iglu.core.SchemaKey

package object lakes {
  implicit val orderingSchemaKey: Ordering[SchemaKey] = SchemaKey.ordering

  type AnyConfig = Config[Any, Any]
}
