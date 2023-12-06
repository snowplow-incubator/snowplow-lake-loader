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

import org.apache.hadoop.conf.Configuration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, AWSSessionCredentials}
import software.amazon.awssdk.auth.credentials.{AwsSessionCredentials => AwsSessionCredentialsV2}

import java.net.URI

/**
 * A credentials provider that delegates to the AWS SDK v2 provider
 *
 * Hadoop is half-way through migrating from AWS SDK v1 to v2. We can delete this wrapper when the
 * migration is complete. Until then, we need a provider that returns a v1 class.
 */
class AssumedRoleCredentialsProviderV1(delegate: AssumedRoleCredentialsProvider) extends AWSCredentialsProvider {

  def this(fsUri: URI, conf: Configuration) =
    this(new AssumedRoleCredentialsProvider(fsUri, conf))

  override def getCredentials(): AWSCredentials = {
    val v2 = delegate.resolveCredentials().asInstanceOf[AwsSessionCredentialsV2]
    new AWSSessionCredentials {
      def getSessionToken()   = v2.sessionToken()
      def getAWSAccessKeyId() = v2.accessKeyId()
      def getAWSSecretKey()   = v2.secretAccessKey()
    }
  }

  override def refresh(): Unit = ()

}
