/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes

import java.util.Date

import com.azure.core.credential.TokenRequestContext
import com.azure.identity.DefaultAzureCredentialBuilder

import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee
import org.apache.hadoop.conf.Configuration

/**
 * Creates Azure tokens for using with Hadoop file system. This class if referenced by the spark
 * config setting `fs.azure.account.oauth.provider.type`
 */
class AzureTokenProvider extends CustomTokenProviderAdaptee {

  private var expiryTime: Date = _
  private var accountName: String = _

  override def initialize(configuration: Configuration, accountName: String): Unit =
    this.accountName = accountName

  override def getAccessToken: String = {
    val creds = new DefaultAzureCredentialBuilder().build()
    val request = new TokenRequestContext().addScopes(s"https://$accountName/.default")
    val token = creds.getToken(request).block()
    this.expiryTime = new Date(token.getExpiresAt.toInstant.toEpochMilli)
    token.getToken
  }

  override def getExpiryTime: Date = expiryTime
}
