/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

object Containers {

  val LOCALSTACK_EXPOSED_PORT          = 4566
  val testStream1Name                  = "test-stream-1"
  val kinesisInitializeStreams: String = List(s"$testStream1Name:1").mkString(",")

  val localstack: LocalStackContainer = {
    val localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
    localstack.withServices(Service.KINESIS)
    localstack.addEnv("KINESIS_INITIALIZE_STREAMS", kinesisInitializeStreams)
    localstack.addExposedPort(LOCALSTACK_EXPOSED_PORT)
    localstack.setWaitStrategy(Wait.forLogMessage(".*Ready.*", 1))
    localstack
  }

}
