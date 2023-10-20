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

import scala.concurrent.duration.FiniteDuration

import com.snowplowanalytics.snowplow.loaders.{Metrics => CommonMetrics}

trait Metrics[F[_]] {
  def addReceived(count: Int): F[Unit]
  def addBad(count: Int): F[Unit]
  def addCommitted(count: Int): F[Unit]
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
    processingLatency: Option[FiniteDuration]
  ) extends CommonMetrics.State {
    def toKVMetrics: List[CommonMetrics.KVMetric] =
      List(
        KVMetric.CountReceived(received),
        KVMetric.CountBad(bad),
        KVMetric.CountCommitted(committed)
      ) ++ processingLatency.map(KVMetric.ProcessingLatency(_))
  }

  private object State {
    def empty: State = State(0, 0, 0, None)
  }

  private def impl[F[_]: Async](config: Config.Metrics, ref: Ref[F, State]): Metrics[F] =
    new CommonMetrics[F, State](ref, State.empty, config.statsd) with Metrics[F] {
      def addReceived(count: Int): F[Unit] =
        ref.update(s => s.copy(received = s.received + count))
      def addBad(count: Int): F[Unit] =
        ref.update(s => s.copy(bad = s.bad + count))
      def addCommitted(count: Int): F[Unit] =
        ref.update(s => s.copy(committed = s.committed + count))
      def setProcessingLatency(latency: FiniteDuration): F[Unit] =
        ref.update { state =>
          val newLatency = state.processingLatency.fold(latency)(_.max(latency))
          state.copy(processingLatency = Some(newLatency))
        }
    }

  private object KVMetric {

    final case class CountReceived(v: Int) extends CommonMetrics.KVMetric {
      val key = "events_received"
      val value = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountBad(v: Int) extends CommonMetrics.KVMetric {
      val key = "events_bad"
      val value = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountCommitted(v: Int) extends CommonMetrics.KVMetric {
      val key = "events_committed"
      val value = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class ProcessingLatency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key = "processing_latency_seconds"
      val value = d.toSeconds.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

  }
}
