/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources

import cats.effect.testing.specs2.CatsEffect
import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global

import org.slf4j.LoggerFactory

import org.testcontainers.containers.output.Slf4jLogConsumer

import org.specs2.mutable.Specification

import software.amazon.awssdk.regions.Region

import scala.annotation.nowarn
import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.snowplow.sources.EventProcessingConfig.NoWindowing
import com.snowplowanalytics.snowplow.sources.Utils._

@nowarn("msg=unused value of type org.specs2.specification.core.Fragment")
class KinesisSourceSpec extends Specification with CatsEffect {

  "Kinesis source" should {
    "read from input stream" in {
      val inputStreamName = "read-input-test-stream"
      val shardCount = 1
      val localstack = getLocalstackContainer(4566, inputStreamName, shardCount)
      localstack.start()

      val logger = LoggerFactory.getLogger("kinesis-source-test")
      val logs = new Slf4jLogConsumer(logger)
      localstack.followOutput(logs)

      val region = Region.of(localstack.getRegion)
      val config = getKinesisConfig(localstack.getEndpoint, localstack.getRegion, inputStreamName)

      val examplePayload = "example-payload"

      val prog = for {
        refProcessed <- Ref[IO].of[List[String]](Nil)
        _ <- putDataToKinesis(getKinesisClient(localstack.getEndpoint, region), inputStreamName, examplePayload)
        sourceAndAck = KinesisSource.build[IO](config).stream(new EventProcessingConfig(NoWindowing), testProcessor(refProcessed))
        fiber <- sourceAndAck.compile.drain.start
        _ <- IO.sleep(2.minutes)
        processed <- refProcessed.get
        _ <- fiber.cancel
      } yield processed must contain(examplePayload)

      prog.unsafeRunSync()
    }
  }
}
