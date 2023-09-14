/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.effect.{IO, Ref}
import com.snowplowanalytics.snowplow.sources.{EventProcessor, TokenedEvents}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{PutRecordRequest, PutRecordResponse}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID

object Utils {
  val awsRegion: IO[Region] = IO(Region.of(Containers.localstack.getRegion))

  def testProcessor(ref: Ref[IO, List[String]]): EventProcessor[IO] =
    _.evalMap { case TokenedEvents(events, token) =>
      for {
        _ <- ref.update(_ ::: events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer).toString))
      } yield token
    }

  def getKinesisClient(endpoint: URI, region: Region): IO[KinesisAsyncClient] =
    IO.blocking(
      KinesisAsyncClient
        .builder()
        .endpointOverride(endpoint)
        .region(region)
        .build()
    )

  def putDataToKinesis(
    client: KinesisAsyncClient,
    streamName: String,
    data: String
  ): IO[PutRecordResponse] = {
    val record = PutRecordRequest
      .builder()
      .streamName(streamName)
      .data(SdkBytes.fromUtf8String(data))
      .partitionKey(UUID.randomUUID().toString)
      .build()

    IO.blocking(client.putRecord(record).get())
  }

  def getKinesisConfig(region: Region): KinesisSourceConfig = KinesisSourceConfig(
    UUID.randomUUID().toString,
    Containers.testStream1Name,
    Some(region),
    KinesisSourceConfig.InitPosition.TrimHorizon,
    KinesisSourceConfig.Retrieval.Polling(1),
    1,
    Some(Containers.localstack.getEndpoint),
    Some(Containers.localstack.getEndpoint),
    Some(Containers.localstack.getEndpoint)
  )
}
