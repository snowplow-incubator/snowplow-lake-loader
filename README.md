# Snowplow Lake Loader

For developers:

```bash
sbt 'project azure; Docker / publishLocal'

docker run snowplow/lake-loader-azure --help

docker run --rm \
  --network=host \
  -v$PWD/config:/config \
  snowplow/lake-loader-azure \
  --config /config/config.azure.minimal.hocon \
  --iglu-config /config/iglu.hocon

```

## Modules

#### streams

The `streams` module is a library that provides an abstraction over all the streams we use at Snowplow. This will be split out of this repo to become its own library.  It should be imported by all Snowplow scala apps.

It handles checkpointing/acking, clean shutdown, batching and windowing.

#### kafka

The `kafka` module provides an implementation of the `streams` api.  This will also be split out to become its own library.

#### core

The `core` module is the main application logic for this loader.  It implements the processing of the events we read from the stream, and how to write to the lake.

#### azure

The `azure` module is an application module that bundles the relevant libraries for Azure.  It produces a docker image which can read from Kafka and write to Azure ADLS.
