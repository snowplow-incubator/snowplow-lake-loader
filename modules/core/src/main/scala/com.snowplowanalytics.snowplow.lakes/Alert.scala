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

import com.snowplowanalytics.snowplow.runtime.SetupExceptionMessages

sealed trait Alert
object Alert {

  final case class FailedToCreateEventsTable(causes: SetupExceptionMessages) extends Alert
  final case class FailedToCommitEvents(causes: SetupExceptionMessages) extends Alert

  implicit def showAlert: Show[Alert] = Show[Alert] {
    case FailedToCreateEventsTable(causes) => show"Failed to create events table: $causes"
    case FailedToCommitEvents(causes)      => show"Failed to write events into table: $causes"
  }
}
