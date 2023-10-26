/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.lakes

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.implicits._
import fs2.Stream

import scala.concurrent.duration.{Duration, FiniteDuration}

import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}

trait Metrics[F[_]] {
  def addReceived(count: Int): F[Unit]
  def addBad(count: Int): F[Unit]
  def addCommitted(count: Int): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setProcessingLatency(latency: FiniteDuration): F[Unit]

  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics): F[Metrics[F]] =
    Ref[F].of(State.empty).map(impl(config, _))

  private case class State(
    received: Int,
    bad: Int,
    committed: Int,
    latency: FiniteDuration,
    processingLatency: Option[FiniteDuration]
  ) extends CommonMetrics.State {
    def toKVMetrics: List[CommonMetrics.KVMetric] =
      List(
        KVMetric.CountReceived(received),
        KVMetric.CountBad(bad),
        KVMetric.CountCommitted(committed),
        KVMetric.Latency(latency)
      ) ++ processingLatency.map(KVMetric.ProcessingLatency(_))
  }

  private object State {
    def empty: State = State(0, 0, 0, Duration.Zero, None)
  }

  private def impl[F[_]: Async](config: Config.Metrics, ref: Ref[F, State]): Metrics[F] =
    new CommonMetrics[F, State](ref, State.empty, config.statsd) with Metrics[F] {
      def addReceived(count: Int): F[Unit] =
        ref.update(s => s.copy(received = s.received + count))
      def addBad(count: Int): F[Unit] =
        ref.update(s => s.copy(bad = s.bad + count))
      def addCommitted(count: Int): F[Unit] =
        ref.update(s => s.copy(committed = s.committed + count))
      def setLatency(latency: FiniteDuration): F[Unit] =
        ref.update(s => s.copy(latency = s.latency.max(latency)))
      def setProcessingLatency(latency: FiniteDuration): F[Unit] =
        ref.update { state =>
          val newLatency = state.processingLatency.fold(latency)(_.max(latency))
          state.copy(processingLatency = Some(newLatency))
        }
    }

  private object KVMetric {

    final case class CountReceived(v: Int) extends CommonMetrics.KVMetric {
      val key        = "events_received"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountBad(v: Int) extends CommonMetrics.KVMetric {
      val key        = "events_bad"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountCommitted(v: Int) extends CommonMetrics.KVMetric {
      val key        = "events_committed"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class Latency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key        = "latency_millis"
      val value      = d.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    final case class ProcessingLatency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key        = "processing_latency_millis"
      val value      = d.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

  }
}
