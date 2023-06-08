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
