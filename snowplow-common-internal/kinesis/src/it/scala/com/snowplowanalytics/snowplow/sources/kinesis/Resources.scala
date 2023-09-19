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
import org.slf4j.LoggerFactory
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

import java.net.URI
import java.nio.charset.StandardCharsets

object Resources {

  def prepareLocalstackContainer(region: Region, kinesisInitializeStreams: String): LocalStackContainer = {
    val localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
    localstack.addEnv("AWS_DEFAULT_REGION", region.id)
    localstack.addEnv("KINESIS_INITIALIZE_STREAMS", kinesisInitializeStreams)
    localstack.addExposedPort(4566)
    localstack.setWaitStrategy(Wait.forLogMessage(".*Ready.*", 1))
    localstack
  }

  def startLocalstack(localstack: LocalStackContainer): LocalStackContainer = {
    localstack.start()
    val logger = LoggerFactory.getLogger(KinesisSourceSpec.getClass.getSimpleName)
    val logs   = new Slf4jLogConsumer(logger)
    localstack.followOutput(logs)
    localstack
  }

  def testProcessor(ref: Ref[IO, List[String]]): EventProcessor[IO] =
    _.evalMap { case TokenedEvents(events, token) =>
      for {
        _ <- ref.update(_ ::: events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer).toString))
      } yield token
    }

  def getKinesisClient(endpoint: URI, region: Region): IO[KinesisAsyncClient] =
    IO(
      KinesisAsyncClient
        .builder()
        .endpointOverride(endpoint)
        .region(region)
        .build()
    )

}
