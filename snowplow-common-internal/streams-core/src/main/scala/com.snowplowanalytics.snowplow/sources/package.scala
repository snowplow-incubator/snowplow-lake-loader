/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow

import fs2.Pipe
import cats.effect.kernel.Unique

package object sources {

  /**
   * An application that processes a source of events
   *
   * The [[EventProcessor]] is implemented by the specific application (e.g. Enrich or Transformer).
   * Once implemented, we can create a runnable program by pairing it with a [[SourceAndAck]].
   *
   * The [[SourceAndAck]] provides the [[EventProcessor]] with events and tokens. The
   * [[EventProcessor]] must emit the tokens after it has fully processed the events.
   */
  type EventProcessor[F[_]] = Pipe[F, TokenedEvents, Unique.Token]

}
