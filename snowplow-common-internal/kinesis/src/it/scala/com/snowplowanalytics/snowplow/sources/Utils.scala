/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources

import cats.effect.{IO, Ref}

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{PutRecordRequest, PutRecordResponse}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID

object Utils {

  val region: Region = Region.of("eu-central-1")

  def testProcessor(ref: Ref[IO, List[String]]): EventProcessor[IO] =
    _.evalMap { case TokenedEvents(events, token) =>
      for {
        _ <- ref.update(_ ::: events.map(bytes => new String(bytes, StandardCharsets.UTF_8)))
      } yield token
    }

  def getLocalstackContainer(
    exposedPort: Int,
    streamName: String,
    shardCount: Int
  ): LocalStackContainer = {
    val localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
    localstack.withServices(Service.KINESIS)
    localstack.addEnv("KINESIS_INITIALIZE_STREAMS", s"$streamName:$shardCount")
    localstack.addExposedPort(exposedPort)
    localstack.setWaitStrategy(Wait.forLogMessage(".*Ready.*", 1))
    localstack
  }

  def getKinesisClient(endpoint: URI, region: Region): KinesisAsyncClient =
    KinesisAsyncClient
      .builder()
      .endpointOverride(endpoint)
      .region(region)
      .build()

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

  def getKinesisConfig(
    endpoint: URI,
    streamName: String
  ): KinesisSourceConfig = KinesisSourceConfig(
    UUID.randomUUID().toString,
    streamName,
    Some(region),
    KinesisSourceConfig.InitPosition.TrimHorizon,
    KinesisSourceConfig.Retrieval.Polling(1),
    1,
    Some(endpoint),
    Some(endpoint),
    Some(endpoint)
  )
}
