/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import org.apache.iceberg.exceptions.{ForbiddenException => IcebergForbiddenException, NotFoundException => IcebergNotFoundException}

import org.apache.spark.sql.delta.DeltaIOException

object TableFormatSetupError {

  // Check if given exception is specific to iceberg format
  def check(t: Throwable): Boolean =
    t match {
      case _: IcebergNotFoundException =>
        // Glue catalog does not exist
        true
      case _: IcebergForbiddenException =>
        // No permission to create a table in Glue catalog
        true
      case _: DeltaIOException =>
        // no read/write permission in s3 bucket
        true
      case _ =>
        false
    }
}