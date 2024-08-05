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
import org.apache.hadoop.fs.s3a.{CredentialInitializationException, UnknownStoreException}
import software.amazon.awssdk.services.s3.model.{NoSuchBucketException, S3Exception}
import software.amazon.awssdk.services.sts.model.StsException
import software.amazon.awssdk.services.glue.model.{
  AccessDeniedException => GlueAccessDeniedException,
  EntityNotFoundException => GlueEntityNotFoundException
}

import java.nio.file.AccessDeniedException
import scala.util.matching.Regex

import com.snowplowanalytics.snowplow.sources.kinesis.{KinesisSource, KinesisSourceConfig}
import com.snowplowanalytics.snowplow.sinks.kinesis.{KinesisSink, KinesisSinkConfig}

object AwsApp extends LoaderApp[KinesisSourceConfig, KinesisSinkConfig](BuildInfo) {

  override def source: SourceProvider = KinesisSource.build(_)

  override def badSink: SinkProvider = KinesisSink.resource(_)

  /**
   * Identifies known exceptions relating to setup of the destination
   *
   * Exceptions are often "caused by" an underlying exception. For example, a s3a
   * UnknownStoreException is often "caused by" a aws sdk NoSuchBucketException. Our implementation
   * checks both the top exception and the underlying causes. Therefore in some cases we
   * over-specify the exceptions to watch out for; the top exception and causal exception both match
   */
  override def isDestinationSetupError: DestinationSetupErrorCheck = {

    /** Exceptions raised by underlying AWS SDK * */
    case _: NoSuchBucketException =>
      // S3 bucket does not exist
      Some("S3 bucket does not exist or we do not have permissions to see it exists")
    case e: S3Exception if e.statusCode() === 403 =>
      // No permission to read from S3 bucket or to write to S3 bucket
      Some("Missing permissions to perform this action on S3 bucket")
    case e: S3Exception if e.statusCode() === 301 =>
      // Misconfigured AWS region
      Some("S3 bucket is not in the expected region")
    case e: GlueAccessDeniedException =>
      // No permission to read from Glue catalog
      Some(Option(e.getMessage).getOrElse("Missing permissions to perform this action on Glue catalog"))
    case _: GlueEntityNotFoundException =>
      // Glue database does not exist
      Some("Glue resource does not exist or no permission to see it exists")
    case e: StsException if e.statusCode() === 403 =>
      // No permission to assume the role given to authenticate to S3/Glue
      Some("Missing permissions to assume the AWS IAM role")

    /** Exceptions raised via hadoop's s3a filesystem * */
    case e: UnknownStoreException =>
      // S3 bucket does not exist or no permission to see it exists
      stripCauseDetails(e)
    case e: AccessDeniedException =>
      // 1 - No permission to put object on the bucket
      // 2 - No permission to assume the role given to authenticate to S3
      stripCauseDetails(e)
    case _: CredentialInitializationException =>
      Some("Failed to initialize AWS access credentials")

    /** Exceptions common to the table format - Delta/Iceberg/Hudi * */
    case t => TableFormatSetupError.check(t)
  }

  /**
   * Fixes hadoop Exception messages to be more reader-friendly
   *
   * Hadoop exception messages often add the exception's cause to the exception's message.
   *
   * E.g. "<HELPFUL MESSAGE>: <CAUSE CLASSNAME>: <CAUSE MESSAGE>"
   *
   * In order to have better control of the message sent to the webhook, we remove the cause details
   * here, and add back in pertinent cause information later.
   */
  private def stripCauseDetails(t: Throwable): Option[String] =
    (Option(t.getMessage), Option(t.getCause)) match {
      case (Some(message), Some(cause)) =>
        val toRemove = new Regex(":? *" + Regex.quote(cause.toString) + ".*")
        val replaced = toRemove.replaceAllIn(message, "")
        Some(replaced)
      case (other, _) =>
        other
    }
}
