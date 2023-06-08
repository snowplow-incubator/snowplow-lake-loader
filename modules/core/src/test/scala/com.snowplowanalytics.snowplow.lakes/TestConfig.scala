package com.snowplowanalytics.snowplow.lakes

import java.nio.file.Path
import scala.concurrent.duration.DurationInt

object TestConfig {

  def ofLocalDirectory(tmp: Path): Config[Unit, Unit] = Config(
    input = (),
    output = Config.Output(target(tmp), ()),
    inMemHeapFraction = 0.0,
    windows = 5.minutes,
    spark = sparkConfig(tmp)
  )

  private def sparkConfig(tmp: Path) = Config.Spark(
    localDir = tmp.resolve("local").toString,
    retries = 1,
    targetParquetSizeMB = 1000,
    threads = 1,
    conf = Map.empty
  )

  private def target(tmp: Path) = Config.Delta(tmp.resolve("events").toUri)
}
