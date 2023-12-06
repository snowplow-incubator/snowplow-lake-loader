/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes

import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.auth.credentials.{AwsCredentials, AwsCredentialsProvider}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.{Constants => S3aConstants}

import java.net.URI
import java.util.concurrent.TimeUnit

/**
 * A credentials provider that uses STS to assume a role
 *
 * Similar to hadoop's in-built `AssumedRoleCredentialsProvider` but with support for an external id
 *
 * @param delegate
 *   The configured credentials provider to which we delegate requests for credentials
 */
class AssumedRoleCredentialsProvider(delegate: StsAssumeRoleCredentialsProvider) extends AwsCredentialsProvider {

  /**
   * Standard constructor invoked by hadoop
   *
   * @param fsUri
   *   Base URI of this filesystem (not used by us)
   * @param conf
   *   The hadoop configuration, provided via spark configuration
   */
  def this(fsUri: URI, conf: Configuration) =
    this(
      StsAssumeRoleCredentialsProvider.builder
        .stsClient {
          StsClient.builder.defaultsMode(DefaultsMode.AUTO).build
        }
        .refreshRequest { (req: AssumeRoleRequest.Builder) =>
          req
            .roleArn(conf.getTrimmed(S3aConstants.ASSUMED_ROLE_ARN))
            .roleSessionName(conf.getTrimmed(S3aConstants.ASSUMED_ROLE_SESSION_NAME))
            .durationSeconds(conf.getTimeDuration(S3aConstants.ASSUMED_ROLE_SESSION_DURATION, 0L, TimeUnit.SECONDS).toInt)
            .externalId(conf.getTrimmed("fs.s3a.assumed.role.session.external.id"))
          ()
        }
        .build
    )

  override def resolveCredentials(): AwsCredentials =
    delegate.resolveCredentials()

}
