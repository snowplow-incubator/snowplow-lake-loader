/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow

import com.snowplowanalytics.iglu.core.SchemaKey

package object loaders {

  private[loaders] implicit val orderingSchemaKey: Ordering[SchemaKey] = SchemaKey.ordering

  /* Represents schema revision and addition for a given schema model */
  type SchemaSubVersion = (Int, Int)
}
