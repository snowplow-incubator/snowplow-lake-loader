/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll

import scala.annotation.nowarn
import scala.concurrent.duration.DurationInt

import Containers._
import Utils._

import com.snowplowanalytics.snowplow.sources.EventProcessingConfig
import com.snowplowanalytics.snowplow.sources.EventProcessingConfig.NoWindowing

@nowarn("msg=unused value of type org.specs2.specification.core.Fragment")
class KinesisSourceSpec extends Specification with BeforeAfterAll {

  def beforeAll(): Unit = localstack.start()
  def afterAll(): Unit  = localstack.stop()

  "Kinesis source" should {
    "read from input stream" in {
      val testPayload = "test-payload"

      val prog = for {
        region <- awsRegion
        refProcessed <- Ref[IO].of[List[String]](Nil)
        kinesisClient <- getKinesisClient(localstack.getEndpoint, region)
        _ <- putDataToKinesis(kinesisClient, Containers.testStream1Name, testPayload)
        processingConfig = new EventProcessingConfig(NoWindowing)
        kinesisConfig    = getKinesisConfig(region)
        sourceAndAck     = KinesisSource.build[IO](kinesisConfig).stream(processingConfig, testProcessor(refProcessed))
        fiber <- sourceAndAck.compile.drain.start
        _ <- IO.sleep(2.minutes)
        processed <- refProcessed.get
        _ <- fiber.cancel
      } yield processed must contain(testPayload)

      prog.unsafeRunSync()

    }
  }
}
