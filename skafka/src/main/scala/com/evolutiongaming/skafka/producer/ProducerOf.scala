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

  /** The sole purpose of this method is to support binary compatibility with an intermediate version (namely, 15.2.0)
    * which had `apply1` method using `MeasureDuration` from `smetrics` and `apply2` using `MeasureDuration` from
    * `cats-helper`. This should not be used and should be removed in a reasonable amount of time.
    */
  @deprecated("Use `apply1`", since = "16.0.3")
  def apply2[F[_]: MeasureDuration: ToTry: Async](
    metrics: Option[ProducerMetrics[F]] = None
  ): ProducerOf[F] = apply1(metrics)

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
