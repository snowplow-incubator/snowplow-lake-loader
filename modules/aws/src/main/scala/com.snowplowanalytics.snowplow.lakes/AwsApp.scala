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

import software.amazon.awssdk.services.s3.model.{NoSuchBucketException, S3Exception}
import software.amazon.awssdk.services.sts.model.StsException
import software.amazon.awssdk.services.glue.model.{AccessDeniedException => GlueAccessDeniedException}

import org.apache.hadoop.fs.s3a.UnknownStoreException

import java.nio.file.AccessDeniedException

import com.snowplowanalytics.snowplow.sources.kinesis.{KinesisSource, KinesisSourceConfig}
import com.snowplowanalytics.snowplow.sinks.kinesis.{KinesisSink, KinesisSinkConfig}

object AwsApp extends LoaderApp[KinesisSourceConfig, KinesisSinkConfig](BuildInfo) {

  override def source: SourceProvider = KinesisSource.build(_)

  override def badSink: SinkProvider = KinesisSink.resource(_)

  override def isDestinationSetupError: DestinationSetupErrorCheck = {
    case _: NoSuchBucketException =>
      // S3 bucket does not exist
      true
    case e: S3Exception if e.statusCode() >= 400 && e.statusCode() < 500 =>
      // No permission to read from S3 bucket or to write to S3 bucket
      true
    case _: GlueAccessDeniedException =>
      // No permission to read from Glue catalog
      true
    case _: StsException =>
      // No permission to assume the role given to authenticate to S3/Glue
      true
    case _: UnknownStoreException =>
      // no such bucket exist
      true
    case _: AccessDeniedException =>
      // 1 - s3 bucket's permission policy denies all actions
      // 2 - not authorized to assume the role
      true
    case t => TableFormatSetupError.check(t)
  }
}
