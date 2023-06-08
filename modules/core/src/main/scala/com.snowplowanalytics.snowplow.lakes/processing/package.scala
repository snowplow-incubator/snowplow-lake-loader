package com.snowplowanalytics.snowplow.lakes

import com.snowplowanalytics.iglu.core.SchemaKey

package object processing {
  implicit val orderingSchemaKey: Ordering[SchemaKey] = SchemaKey.ordering

  type SchemaSubVersion = (Int, Int)
}
