/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
