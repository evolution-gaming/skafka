package com.evolutiongaming.skafka.producer

import cats.effect.{Concurrent, ContextShift, Effect, Resource}
import cats.{Defer, Monad, ~>}
import com.evolutiongaming.catshelper.{MeasureDuration, ToTry}
import com.evolutiongaming.smetrics

import scala.concurrent.ExecutionContext

trait ProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, Producer[F]]
}

object ProducerOf {

  @deprecated("use `apply1` instead", "11.7.0")
  def apply[F[_]: Effect: ContextShift: smetrics.MeasureDuration](
    executorBlocking: ExecutionContext,
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = new ProducerOf[F] {

    def apply(config: ProducerConfig) = {
      for {
        producer <- Producer.of[F](config, executorBlocking)
      } yield {
        metrics.fold(producer)(producer.withMetrics[Throwable])
      }
    }
  }

  @deprecated("Use apply2", since = "11.15.1")
  def apply1[F[_]: Concurrent: ContextShift: smetrics.MeasureDuration: ToTry](
    executorBlocking: ExecutionContext,
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = {
    implicit val md: MeasureDuration[F] = smetrics.MeasureDuration[F].toCatsHelper
    apply2(executorBlocking, metrics)
  }

  def apply2[F[_]: Concurrent: ContextShift: MeasureDuration: ToTry](
    executorBlocking: ExecutionContext,
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = new ProducerOf[F] {

    def apply(config: ProducerConfig) = {
      for {
        producer <- Producer.of1[F](config, executorBlocking)
      } yield {
        metrics.fold(producer)(producer.withMetrics1[Throwable])
      }
    }
  }

  implicit class ProducerOfOps[F[_]](val self: ProducerOf[F]) extends AnyVal {

    def mapK[G[_]: Monad: Defer](
      fg: F ~> G,
      gf: G ~> F
    )(implicit F: Monad[F]): ProducerOf[G] = { (config: ProducerConfig) =>
      {
        for {
          a <- self(config).mapK(fg)
        } yield {
          a.mapK(fg, gf)
        }
      }
    }
  }
}
