/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.lakes.processing

/**
 * Information about a Spark DataFrame temporarily cached on local disk
 *
 * When the time window closes, all pending saved DataFrames will be combined and flushed to cloud
 * storage
 *
 * @param viewName
 *   the name by which the DataFrame is known to the Spark catalog
 * @param count
 *   the number of events in this DataFrame
 */
final case class DataFrameOnDisk(viewName: String, count: Int)
