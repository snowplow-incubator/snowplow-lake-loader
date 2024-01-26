/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.sinks

/**
 * A single event that can be written to the external sink
 *
 * @param bytes
 *   the serialized content of this event
 * @param partitionKey
 *   optionally controls which partition the event is written to
 * @param attributes
 *   optionally add attributes/headers to the event, if the sink supports this feature
 */
case class Sinkable(
  bytes: Array[Byte],
  partitionKey: Option[String],
  attributes: Map[String, String]
)
