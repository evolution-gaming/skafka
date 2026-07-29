package com.evolutiongaming.skafka.metrics

import cats.effect.{Ref, Resource, Sync}
import cats.syntax.all.*
import com.evolutiongaming.smetrics.*

final class InMemoryCollectorRegistry[F[_]] private (stateRef: Ref[F, InMemoryCollectorRegistry.State])
    extends CollectorRegistry[F] {
  import InMemoryCollectorRegistry.*

  def state: F[State] = stateRef.get

  def gauge[A, B[_]](name: String, help: String, labels: A)(
    implicit magnet: LabelsMagnet[A, B]
  ): Resource[F, B[Gauge[F]]] =
    Resource.pure(magnet.withValues { labelValues =>
      val key = MetricKey(name, labelValues)
      new Gauge[F] {
        def inc(value: Double): F[Unit] = stateRef.update(_.incGauge(key, value))
        def dec(value: Double): F[Unit] = stateRef.update(_.incGauge(key, -value))
        def set(value: Double): F[Unit] = stateRef.update(_.setGauge(key, value))
      }
    })

  def gaugeInitialized[A, B[_]](
    name: String,
    help: String,
    labels: A,
  )(implicit magnet: LabelsMagnetInitialized[A, B]): Resource[F, B[Gauge[F]]] = gauge(name, help, labels)

  def counter[A, B[_]](
    name: String,
    help: String,
    labels: A,
  )(implicit magnet: LabelsMagnet[A, B]): Resource[F, B[Counter[F]]] =
    Resource.pure(magnet.withValues { labelValues =>
      val key = MetricKey(name, labelValues)
      (value: Double) => stateRef.update(_.incCounter(key, value))
    })

  def counterInitialized[A, B[_]](
    name: String,
    help: String,
    labels: A,
  )(implicit magnet: LabelsMagnetInitialized[A, B]): Resource[F, B[Counter[F]]] = counter(name, help, labels)

  def summary[A, B[_]](
    name: String,
    help: String,
    quantiles: Quantiles,
    labels: A,
  )(implicit magnet: LabelsMagnet[A, B]): Resource[F, B[Summary[F]]] =
    Resource.pure(magnet.withValues { labelValues =>
      val key = MetricKey(name, labelValues)
      (value: Double) => stateRef.update(_.addObservation(key, value))
    })

  def summaryInitialized[A, B[_]](
    name: String,
    help: String,
    quantiles: Quantiles,
    labels: A,
  )(implicit magnet: LabelsMagnetInitialized[A, B]): Resource[F, B[Summary[F]]] =
    summary(name, help, quantiles, labels)

  def histogram[A, B[_]](
    name: String,
    help: String,
    buckets: Buckets,
    labels: A,
  )(implicit magnet: LabelsMagnet[A, B]): Resource[F, B[Histogram[F]]] =
    Resource.pure(magnet.withValues { labelValues =>
      val key = MetricKey(name, labelValues)
      (value: Double) => stateRef.update(_.addObservation(key, value))
    })

  def histogramInitialized[A, B[_]](
    name: String,
    help: String,
    buckets: Buckets,
    labels: A,
  )(implicit magnet: LabelsMagnetInitialized[A, B]): Resource[F, B[Histogram[F]]] =
    histogram(name, help, buckets, labels)

  def info[A, B[_]](name: String, help: String, labels: A)(
    implicit magnet: LabelsMagnet[A, B]
  ): Resource[F, B[Info[F]]] =
    Resource.pure(magnet.withValues { labelValues =>
      val key = MetricKey(name, labelValues)
      new Info[F] {
        def set(): F[Unit] = stateRef.update(_.setInfo(key))
      }
    })
}

object InMemoryCollectorRegistry {

  final case class MetricKey(name: String, labels: List[String])

  final case class State(
    gauges: Map[MetricKey, Double]             = Map.empty,
    counters: Map[MetricKey, Double]           = Map.empty,
    observations: Map[MetricKey, List[Double]] = Map.empty,
    infos: Set[MetricKey]                      = Set.empty,
  ) {

    def incGauge(key: MetricKey, value: Double): State =
      copy(gauges = gauges.updated(key, gauges.getOrElse(key, 0.0) + value))

    def setGauge(key: MetricKey, value: Double): State =
      copy(gauges = gauges.updated(key, value))

    def incCounter(key: MetricKey, value: Double): State =
      copy(counters = counters.updated(key, counters.getOrElse(key, 0.0) + value))

    def addObservation(key: MetricKey, value: Double): State =
      copy(observations = observations.updated(key, value :: observations.getOrElse(key, Nil)))

    def setInfo(key: MetricKey): State =
      copy(infos = infos + key)
  }

  def of[F[_]: Sync]: F[InMemoryCollectorRegistry[F]] =
    Ref.of[F, State](State()).map(new InMemoryCollectorRegistry(_))
}
