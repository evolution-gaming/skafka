package com.evolutiongaming.skafka.producer

import cats.effect.{Bracket, ContextShift, Resource, Sync}
import cats.{Defer, Monad, ~>}
import com.evolutiongaming.catshelper.FromFuture

import scala.concurrent.ExecutionContext

trait ProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, Producer[F]]
}

object ProducerOf {

  def apply[F[_] : Sync : ContextShift : FromFuture](
    executorBlocking: ExecutionContext
  ): ProducerOf[F] = new ProducerOf[F] {
    def apply(config: ProducerConfig) = {
      Producer.of[F](config, executorBlocking)
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
