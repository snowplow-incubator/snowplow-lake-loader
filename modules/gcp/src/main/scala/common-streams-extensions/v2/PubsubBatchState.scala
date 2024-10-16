/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.pubsub.v2

import java.time.Instant

/**
 * Data held about a batch of messages pulled from a pubsub subscription
 *
 * @param currentDeadline
 *   The deadline before which we must either ack, nack, or extend the deadline to something further
 *   in the future. This is updated over time if we approach a deadline.
 * @param ackIds
 *   The IDs which are needed to ack all messages in the batch
 */
private case class PubsubBatchState(
  currentDeadline: Instant,
  ackIds: Vector[String]
)
