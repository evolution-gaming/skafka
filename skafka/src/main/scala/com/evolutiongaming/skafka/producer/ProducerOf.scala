package com.evolutiongaming.skafka.producer

import cats.effect.{Async, MonadCancel, Resource}
import cats.~>
import com.evolutiongaming.catshelper.{MeasureDuration, ToTry}

import scala.concurrent.ExecutionContext

trait ProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, Producer[F]]
}

object ProducerOf {

  @deprecated("Use apply1", since = "12.0.1")
  def apply[F[_]: MeasureDuration: ToTry: Async](
    executorBlocking: ExecutionContext,
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = apply1(metrics = metrics)

  def apply1[F[_]: MeasureDuration: ToTry: Async](
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = new ProducerOf[F] {

    def apply(config: ProducerConfig) = {
      for {
        producer <- Producer.of[F](config = config)
      } yield {
        metrics.fold(producer)(producer.withMetrics[Throwable])
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
