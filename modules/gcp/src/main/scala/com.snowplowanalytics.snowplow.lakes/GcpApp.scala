/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import cats.implicits._

import com.google.api.client.auth.oauth2.TokenResponseException
import com.google.api.client.googleapis.json.GoogleJsonResponseException

import com.snowplowanalytics.snowplow.sources.pubsub.{PubsubSource, PubsubSourceConfig}
import com.snowplowanalytics.snowplow.sinks.pubsub.{PubsubSink, PubsubSinkConfig}

object GcpApp extends LoaderApp[PubsubSourceConfig, PubsubSinkConfig](BuildInfo) {

  override def source: SourceProvider = PubsubSource.build(_)

  override def badSink: SinkProvider = PubsubSink.resource(_)

  override def isDestinationSetupError: DestinationSetupErrorCheck = {
    // Bad Request - Key belongs to nonexistent service account
    case e: TokenResponseException if e.getStatusCode === 400 =>
      "The service account key is invalid"
    // Forbidden - Permissions missing for Cloud Storage
    case e: GoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(403) =>
      e.getDetails.getMessage
    // Not Found - Destination bucket doesn't exist
    case e: GoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(404) =>
      "The specified bucket does not exist"
    // Exceptions common to the table format - Delta/Iceberg/Hudi
    case TableFormatSetupError.check(t) =>
      t
  }
}
