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

import cats.Show
import cats.implicits.showInterpolator

import com.snowplowanalytics.iglu.core.circe.implicits.igluNormalizeDataJson
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.runtime.AppInfo

import io.circe.Json
import io.circe.syntax.EncoderOps

sealed trait Alert
object Alert {

  /** Restrict the length of an alert message to be compliant with alert iglu schema */
  private val MaxAlertPayloadLength = 4096

  final case class FailedToCreateEventsTable(causes: List[String]) extends Alert
  final case class FailedToCommitEvents(causes: List[String]) extends Alert

  def toSelfDescribingJson(
    alert: Alert,
    appInfo: AppInfo,
    tags: Map[String, String]
  ): Json =
    SelfDescribingData(
      schema = SchemaKey("com.snowplowanalytics.monitoring.loader", "alert", "jsonschema", SchemaVer.Full(1, 0, 0)),
      data = Json.obj(
        "appName" -> appInfo.name.asJson,
        "appVersion" -> appInfo.version.asJson,
        "message" -> getMessage(alert).asJson,
        "tags" -> tags.asJson
      )
    ).normalize

  private def getMessage(alert: Alert): String = {
    val full = alert match {
      case FailedToCreateEventsTable(causes) => show"Failed to create events table: $causes"
      case FailedToCommitEvents(causes)      => show"Failed to write events into table: $causes"
    }

    full.take(MaxAlertPayloadLength)
  }

  private implicit def causesShow: Show[List[String]] = {
    def removeDuplicateMessages(in: List[String]): List[String] =
      in match {
        case h :: t :: rest =>
          if (h.contains(t)) removeDuplicateMessages(h :: rest)
          else if (t.contains(h)) removeDuplicateMessages(t :: rest)
          else h :: removeDuplicateMessages(t :: rest)
        case fewer => fewer
      }

    Show.show { causes =>
      removeDuplicateMessages(causes).mkString(": ")
    }
  }
}
