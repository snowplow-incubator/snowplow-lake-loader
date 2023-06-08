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
