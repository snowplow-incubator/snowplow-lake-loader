/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
