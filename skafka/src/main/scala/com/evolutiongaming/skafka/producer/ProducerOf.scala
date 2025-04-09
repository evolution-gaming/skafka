package com.evolutiongaming.skafka.producer

import cats.effect.{Async, MonadCancel, Resource}
import cats.~>
import com.evolutiongaming.catshelper.{MeasureDuration, ToTry}

trait ProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, Producer[F]]
}

object ProducerOf {

  def apply1[F[_]: MeasureDuration: ToTry: Async](
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = new ProducerOf[F] {

    def apply(config: ProducerConfig) = {
      for {
        producer <- Producer.of[F](config = config)
        producer <- metrics match {

          case None =>
            Resource.pure[F, Producer[F]](producer)

          case Some(metrics) =>
            for {
              _ <- metrics.exposeJavaMetrics(producer)
            } yield {
              producer.withMetrics[Throwable](metrics)
            }

        }
      } yield producer
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
