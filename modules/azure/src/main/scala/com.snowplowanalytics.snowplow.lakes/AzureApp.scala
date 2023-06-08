package com.snowplowanalytics.snowplow.lakes

import com.snowplowanalytics.snowplow.sources.{KafkaSource, KafkaSourceConfig}
import com.snowplowanalytics.snowplow.sinks.{KafkaSink, KafkaSinkConfig}

object AzureApp extends LoaderApp[KafkaSourceConfig, KafkaSinkConfig](BuildInfo.name, BuildInfo.dockerAlias, BuildInfo.version) {

  override def source: SourceProvider = KafkaSource.build(_)

  override def badSink: SinkProvider = KafkaSink.resource(_)
}
