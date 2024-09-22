/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import com.google.pubsub.v1.ReceivedMessage
import com.google.api.gax.rpc.StreamController
import java.time.Instant

/** Represents the handover of actions from the GRPC observer to the FS2 stream */
private sealed trait SubscriberAction

private object SubscriberAction {

  /** An error was received by the streaming pull observer */
  case class SubscriberError(t: Throwable) extends SubscriberAction

  /**
   * Records were received by the streaming pull observer
   *
   * @param records
   *   The received records
   * @param streamController
   *   The GRPC stream controller. When this action is handed over to cats-effect/fs2 world then we
   *   must tell the stream controller we are ready to receive more events
   * @param timeRecieved
   *   Timestamp the records were pulled over the GRPC stream
   */
  case class ProcessRecords(
    records: Vector[ReceivedMessage],
    streamController: StreamController,
    timeReceived: Instant
  ) extends SubscriberAction
}
