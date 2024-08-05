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

object TableFormatSetupError {

  // Check if given exception is specific to iceberg format
  def check(t: Throwable): Option[String] =
    t match {
      case e: IcebergNotFoundException =>
        // Glue catalog does not exist
        Some(e.getMessage)
      case e: IcebergForbiddenException =>
        // No permission to create a table in Glue catalog
        Some(e.getMessage)
      case _ =>
        None
    }
}
