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

import cats.implicits._

import com.google.api.client.googleapis.json.GoogleJsonResponseException

import com.snowplowanalytics.snowplow.sources.pubsub.{PubsubSource, PubsubSourceAlternative}
import com.snowplowanalytics.snowplow.sources.pubsub.v2.PubsubSourceV2
import com.snowplowanalytics.snowplow.sinks.pubsub.{PubsubSink, PubsubSinkConfig}

object GcpApp extends LoaderApp[PubsubSourceAlternative, PubsubSinkConfig](BuildInfo) {

  override def source: SourceProvider = {
    case PubsubSourceAlternative.V1(c) => PubsubSource.build(c)
    case PubsubSourceAlternative.V2(c) => PubsubSourceV2.build(c)
  }

  override def badSink: SinkProvider = PubsubSink.resource(_)

  override def isDestinationSetupError: DestinationSetupErrorCheck = {
    // Destination bucket doesn't exist
    case e: GoogleJsonResponseException if e.getDetails.getCode === 404 =>
      "The specified bucket does not exist"
    // Permissions missing for Cloud Storage
    case e: GoogleJsonResponseException if e.getDetails.getCode === 403 =>
      e.getDetails.getMessage
    // Exceptions common to the table format - Delta/Iceberg/Hudi
    case TableFormatSetupError.check(t) =>
      t
  }
}
