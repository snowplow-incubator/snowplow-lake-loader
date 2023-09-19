/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.effect.{IO, Ref, Resource}
import cats.effect.testing.specs2.CatsResource

import scala.annotation.nowarn
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import com.snowplowanalytics.snowplow.sources.EventProcessingConfig
import com.snowplowanalytics.snowplow.sources.EventProcessingConfig.NoWindowing
import org.specs2.mutable.SpecificationLike
import org.testcontainers.containers.localstack.LocalStackContainer
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import Utils._
import Resources._

@nowarn("msg=unused value of type org.specs2.specification.core.Fragment")
class KinesisSourceSpec
    extends CatsResource[IO, (LocalStackContainer, KinesisAsyncClient, String => KinesisSourceConfig)]
    with SpecificationLike {
  import KinesisSourceSpec._

  override val Timeout: FiniteDuration = 3.minutes

  /** Resources which are shared across tests */
  override val resource: Resource[IO, (LocalStackContainer, KinesisAsyncClient, String => KinesisSourceConfig)] =
    Resource.make(
      for {
        region <- IO.fromEither(KinesisSourceConfig.getRuntimeRegion)
        lsContainer = prepareLocalstackContainer(region, KINESIS_INITIALIZE_STREAMS)
        localstack  = startLocalstack(lsContainer)
        kinesisClient <- getKinesisClient(localstack.getEndpoint, region)
      } yield (localstack, kinesisClient, getKinesisConfig(localstack.getEndpoint)(_))
    )(res => IO(res._1.stop()))

  "Kinesis source" should {
    "read from input stream" in withResource { case (_, kinesisClient, getKinesisConfig) =>
      val testPayload = "test-payload"

      for {
        refProcessed <- Ref[IO].of[List[String]](Nil)
        _ <- putDataToKinesis(kinesisClient, testStream1Name, testPayload)
        processingConfig = new EventProcessingConfig(NoWindowing)
        kinesisConfig    = getKinesisConfig(testStream1Name)
        sourceAndAck     = KinesisSource.build[IO](kinesisConfig).stream(processingConfig, testProcessor(refProcessed))
        fiber <- sourceAndAck.compile.drain.start
        _ <- IO.sleep(2.minutes)
        processed <- refProcessed.get
        _ <- fiber.cancel
      } yield processed.toSet must beEqualTo(Set(testPayload))
    }
  }
}

object KinesisSourceSpec {
  val testStream1Name = "test-stream-1"
  val KINESIS_INITIALIZE_STREAMS: String =
    List(s"$testStream1Name:1").mkString(",")
}
