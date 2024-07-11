/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes.processing

import cats.implicits._
import cats.data.NonEmptyList
import cats.effect.{IO, Ref}
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.lakes.AppHealth
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck}

import scala.concurrent.duration.{DurationLong, FiniteDuration}

class LakeWriterSpec extends Specification with CatsEffect {
  import LakeWriterSpec._

  def is = s2"""
  The lake writer should:
    become healthy after creating the table $e1
    stay unhealthy after failure to create the table $e2
    stay healthy after committing to the lake $e3
    become unhealthty after failure to commit to the lake $e4
  """

  def e1 =
    control().flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(c.lakeWriter, c.appHealth)

      for {
        healthBefore <- c.appHealth.status
        _ <- wrappedLakeWriter.createTable
        healthAfter <- c.appHealth.status
        state <- c.state.get
      } yield List(
        state should beEqualTo(expected),
        healthBefore should beAnInstanceOf[HealthProbe.Unhealthy],
        healthAfter should beEqualTo(HealthProbe.Healthy)
      ).reduce(_ and _)
    }

  def e2 = {
    val mocks = Mocks(List(Response.ExceptionThrown(new RuntimeException("boom!"))))
    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CreateTableAttempted
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(c.lakeWriter, c.appHealth)

      for {
        healthBefore <- c.appHealth.status
        _ <- wrappedLakeWriter.createTable.voidError
        healthAfter <- c.appHealth.status
        state <- c.state.get
      } yield List(
        state should beEqualTo(expected),
        healthBefore should beAnInstanceOf[HealthProbe.Unhealthy],
        healthAfter should beAnInstanceOf[HealthProbe.Unhealthy]
      ).reduce(_ and _)
    }

  }

  def e3 =
    control().flatMap { c =>
      val expected = Vector(
        Action.CommitAttempted("testview")
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(c.lakeWriter, c.appHealth)

      for {
        healthBefore <- c.appHealth.status
        _ <- wrappedLakeWriter.commit("testview")
        healthAfter <- c.appHealth.status
        state <- c.state.get
      } yield List(
        state should beEqualTo(expected),
        healthBefore should beAnInstanceOf[HealthProbe.Unhealthy],
        healthAfter should beEqualTo(HealthProbe.Healthy)
      ).reduce(_ and _)
    }

  def e4 = {
    val mocks = Mocks(List(Response.Success, Response.ExceptionThrown(new RuntimeException("boom!"))))

    control(mocks).flatMap { c =>
      val expected = Vector(
        Action.CommitAttempted("testview1"),
        Action.CommitAttempted("testview2")
      )

      val wrappedLakeWriter = LakeWriter.withHandledErrors(c.lakeWriter, c.appHealth)

      for {
        _ <- wrappedLakeWriter.commit("testview1")
        healthAfterFirst <- c.appHealth.status
        _ <- wrappedLakeWriter.commit("testview2").voidError
        healthAfterSecond <- c.appHealth.status
        state <- c.state.get
      } yield List(
        state should beEqualTo(expected),
        healthAfterFirst should beEqualTo(HealthProbe.Healthy),
        healthAfterSecond should beAnInstanceOf[HealthProbe.Unhealthy]
      ).reduce(_ and _)
    }
  }

}

object LakeWriterSpec {
  sealed trait Action

  object Action {
    case object CreateTableAttempted extends Action
    case class CommitAttempted(viewName: String) extends Action
  }

  sealed trait Response
  object Response {
    case object Success extends Response
    final case class ExceptionThrown(value: Throwable) extends Response
  }

  case class Mocks(lakeWriterResults: List[Response])

  case class Control(
    state: Ref[IO, Vector[Action]],
    lakeWriter: LakeWriter[IO],
    appHealth: AppHealth[IO]
  )

  def control(mocks: Mocks = Mocks(Nil)): IO[Control] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      appHealth <- testAppHealth
      tableManager <- testLakeWriter(state, mocks.lakeWriterResults)
    } yield Control(state, tableManager, appHealth)

  private def testAppHealth: IO[AppHealth[IO]] = {
    val healthySource = new SourceAndAck[IO] {
      override def stream(config: EventProcessingConfig, processor: EventProcessor[IO]): fs2.Stream[IO, Nothing] =
        fs2.Stream.empty

      override def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO(SourceAndAck.Healthy)
    }
    AppHealth.init(10.seconds, healthySource).flatTap { appHealth =>
      appHealth.setServiceHealth(AppHealth.Service.BadSink, isHealthy = true)
    }
  }

  private def testLakeWriter(state: Ref[IO, Vector[Action]], mocks: List[Response]): IO[LakeWriter[IO]] =
    for {
      mocksRef <- Ref[IO].of(mocks)
    } yield new LakeWriter[IO] {
      def createTable: IO[Unit] =
        for {
          response <- mocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ Action.CreateTableAttempted)
          result <- response match {
                      case Response.Success =>
                        IO.unit
                      case Response.ExceptionThrown(ex) =>
                        IO.raiseError(ex).adaptError { case t =>
                          t.setStackTrace(Array()) // don't clutter our test logs
                          t
                        }
                    }
        } yield result

      def initializeLocalDataFrame(viewName: String): IO[Unit] = IO.unit

      def localAppendRows(
        viewName: String,
        rows: NonEmptyList[Row],
        schema: StructType
      ): IO[Unit] = IO.unit

      def removeDataFrameFromDisk(viewName: String): IO[Unit] = IO.unit

      def commit(viewName: String): IO[Unit] =
        for {
          response <- mocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ Action.CommitAttempted(viewName))
          result <- response match {
                      case Response.Success =>
                        IO.unit
                      case Response.ExceptionThrown(ex) =>
                        IO.raiseError(ex).adaptError { case t =>
                          t.setStackTrace(Array()) // don't clutter our test logs
                          t
                        }
                    }
        } yield result
    }

}
