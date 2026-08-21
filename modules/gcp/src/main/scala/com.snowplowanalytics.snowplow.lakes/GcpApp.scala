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
import cats.effect.IO

// Thrown by Iceberg's GCSFileIO (Iceberg REST catalogs on GCS), which uses the plain
// java-storage/api-client libraries
import com.google.api.client.auth.oauth2.TokenResponseException
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.HttpResponseException
import com.google.cloud.storage.StorageException

// Thrown by hadoop-gcp (the gs scheme for Delta and Hadoop-catalog Iceberg): it ships only as a
// shaded jar that relocates the same libraries, so its exceptions are these repackaged classes
import com.google.cloud.hadoop.repackaged.ossgcs.com.google.api.client.googleapis.json.{
  GoogleJsonResponseException => RepackagedGoogleJsonResponseException
}
import com.google.cloud.hadoop.repackaged.ossgcs.com.google.api.client.http.{HttpResponseException => RepackagedHttpResponseException}
import com.google.cloud.hadoop.repackaged.ossgcs.com.google.cloud.storage.{StorageException => RepackagedStorageException}

import com.snowplowanalytics.snowplow.streams.pubsub.{PubsubFactory, PubsubFactoryConfig, PubsubSinkConfig, PubsubSourceConfig}

object GcpApp extends LoaderApp[PubsubFactoryConfig, PubsubSourceConfig, PubsubSinkConfig](BuildInfo) {

  override def toFactory: FactoryProvider = config => PubsubFactory.resource[IO](config)

  override def isDestinationSetupError: DestinationSetupErrorCheck = {
    // --- Plain classes, thrown on the Iceberg GCSFileIO path ---
    // Bad Request - Key belongs to nonexistent service account
    case e: TokenResponseException if e.getStatusCode === 400 =>
      "The service account key is invalid"
    // Same condition surfaced via google-auth-library. Its GoogleAuthException is package-private,
    // so match the underlying HTTP rejection: the invalid_grant OAuth error identifies a rejected
    // credential, e.g. a key belonging to a deleted service account
    case e: HttpResponseException if e.getStatusCode === 400 && Option(e.getContent).exists(_.contains("invalid_grant")) =>
      "The service account key is invalid"
    // Forbidden - permissions missing for Cloud Storage
    case e: GoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(403) =>
      Option(e.getDetails.getMessage).getOrElse("IAM role is missing permissions")
    // Not Found - destination bucket doesn't exist
    case e: GoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(404) =>
      "The specified bucket does not exist"
    // Forbidden - permissions missing for Cloud Storage
    case e: StorageException if e.getCode === 403 =>
      "IAM role is missing permissions"

    // --- Repackaged equivalents, thrown on the hadoop-gcp path ---
    // Bad Request - the OAuth token endpoint rejected the credential
    case e: RepackagedHttpResponseException if e.getStatusCode === 400 && Option(e.getContent).exists(_.contains("invalid_grant")) =>
      "The service account key is invalid"
    // Not Found - destination bucket doesn't exist
    case e: RepackagedStorageException if e.getCode === 404 =>
      "The specified bucket does not exist"
    // Forbidden - permissions missing for Cloud Storage
    case e: RepackagedStorageException if e.getCode === 403 =>
      Option(e.getMessage).getOrElse("IAM role is missing permissions")
    // On the paths exercised so far the HTTP-transport rejection appears beneath a translated
    // RepackagedStorageException, which the arms above match first. Kept for paths where the
    // java-storage wrapper may be absent (the cause-chain walker visits every level either way)
    case e: RepackagedGoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(404) =>
      "The specified bucket does not exist"
    case e: RepackagedGoogleJsonResponseException if Option(e.getDetails).map(_.getCode).contains(403) =>
      Option(e.getDetails.getMessage).getOrElse("IAM role is missing permissions")
  }
}
