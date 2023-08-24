package com.evolutiongaming.skafka.producer

import cats.effect.{Concurrent, ContextShift, Effect, Resource}
import cats.{Defer, Monad, ~>}
import com.evolutiongaming.catshelper.{MeasureDuration, ToTry}

import scala.concurrent.ExecutionContext

trait ProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, Producer[F]]
}

object ProducerOf {

  @deprecated("use `apply1` instead", "11.7.0")
  def apply[F[_]: Effect: ContextShift: MeasureDuration](
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

  def apply1[F[_]: Concurrent: ContextShift: MeasureDuration: ToTry](
    executorBlocking: ExecutionContext,
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = new ProducerOf[F] {

    def apply(config: ProducerConfig) = {
      for {
        producer <- Producer.of1[F](config, executorBlocking)
      } yield {
        metrics.fold(producer)(producer.withMetrics[Throwable])
      }
    }
  }

  /** The sole purpose of this method is to support binary compatibility with an intermediate
   *  version (namely, 11.15.1) which had `apply1` method using `MeasureDuration` from `smetrics`
   *  and `apply2` using `MeasureDuration` from `cats-helper`.
   *  This should not be used and should be removed in a reasonable amount of time.
   */
  @deprecated("Use `apply1`", since = "11.16.3")
  def apply2[F[_]: Concurrent: ContextShift: MeasureDuration: ToTry](
    executorBlocking: ExecutionContext,
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = apply1(executorBlocking, metrics)

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
