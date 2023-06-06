package com.evolutiongaming.skafka.producer

import cats.effect.{Async, MonadCancel, Resource}
import cats.~>
import com.evolutiongaming.catshelper.{MeasureDuration, ToTry}
import com.evolutiongaming.smetrics

import scala.concurrent.ExecutionContext

trait ProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, Producer[F]]
}

object ProducerOf {

  @deprecated("Use apply1", since = "12.0.1")
  def apply[F[_]: smetrics.MeasureDuration: ToTry: Async](
    executorBlocking: ExecutionContext,
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = apply1(metrics = metrics)

  @deprecated("Use apply2", since = "15.2.0")
  def apply1[F[_]: smetrics.MeasureDuration: ToTry: Async](
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = {
    apply2(metrics)
  }

  def apply2[F[_]: MeasureDuration: ToTry: Async](
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = new ProducerOf[F] {

    def apply(config: ProducerConfig) = {
      for {
        producer <- Producer.of[F](config = config)
      } yield {
        metrics.fold(producer)(producer.withMetrics1[Throwable])
      }
    }
  }

  implicit class ProducerOfOps[F[_]](val self: ProducerOf[F]) extends AnyVal {

    def mapK[G[_]](
      fg: F ~> G,
      gf: G ~> F
    )(implicit G: MonadCancel[G, _], F: MonadCancel[F, _]): ProducerOf[G] = { (config: ProducerConfig) =>
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
