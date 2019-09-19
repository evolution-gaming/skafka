package com.evolutiongaming.skafka.producer

import cats.effect.{Bracket, ContextShift, Effect, Resource}
import cats.{Defer, Monad, ~>}
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.ExecutionContext

trait ProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, Producer[F]]
}

object ProducerOf {

  def apply[F[_] : Effect : ContextShift : MeasureDuration](
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


  implicit class ProducerOfOps[F[_]](val self: ProducerOf[F]) extends AnyVal {

    def mapK[G[_] : Monad : Defer](
      fg: F ~> G,
      gf: G ~> F)(implicit
      B: Bracket[F, Throwable]
    ): ProducerOf[G] = new ProducerOf[G] {

      def apply(config: ProducerConfig) = {
        for {
          a <- self(config).mapK(fg)
        } yield {
          a.mapK(fg, gf)
        }
      }
    }
  }
}
