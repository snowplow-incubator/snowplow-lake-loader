/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.sources

import cats.effect.kernel.Unique

/**
 * The events as they are fed into a [[EventProcessor]]
 *
 * @param events
 *   Each item in the List an event read from the external stream, before parsing
 * @param ack
 *   The [[EventProcessor]] must emit this token after it has fully processed the batch of events.
 *   When the [[EventProcessor]] emits the token, it is an instruction to the [[SourceAndAck]] to
 *   ack/checkpoint the events.
 */
case class TokenedEvents(events: List[Array[Byte]], ack: Unique.Token)
