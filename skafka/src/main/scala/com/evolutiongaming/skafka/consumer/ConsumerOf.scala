package com.evolutiongaming.skafka.consumer

import cats.effect.{Bracket, Concurrent, ContextShift, Resource}
import cats.{Applicative, Defer, ~>}
import com.evolutiongaming.catshelper.{ToFuture, ToTry}
import com.evolutiongaming.skafka.FromBytes

import scala.concurrent.ExecutionContext

trait ConsumerOf[F[_]] {

  def apply[K, V](
    config: ConsumerConfig)(implicit
    fromBytesK: FromBytes[F, K],
    fromBytesV: FromBytes[F, V]
  ): Resource[F, Consumer[F, K, V]]
}

object ConsumerOf {

  def apply[F[_] : Concurrent : ContextShift : ToTry : ToFuture](
    executorBlocking: ExecutionContext
  ): ConsumerOf[F] = new ConsumerOf[F] {

    def apply[K, V](
      config: ConsumerConfig)(implicit
      fromBytesK: FromBytes[F, K],
      fromBytesV: FromBytes[F, V]
    ) = {
      Consumer.of[F, K, V](config, executorBlocking)
    }
  }


  implicit class ConsumerOfOps[F[_]](val self: ConsumerOf[F]) extends AnyVal {

    def mapK[G[_] : Applicative : Defer](
      fg: F ~> G,
      gf: G ~> F)(implicit
      B: Bracket[F, Throwable]
    ): ConsumerOf[G] = new ConsumerOf[G] {

      def apply[K, V](
        config: ConsumerConfig)(implicit
        fromBytesK: FromBytes[G, K],
        fromBytesV: FromBytes[G, V]
      ) = {
        for {
          a <- self[K, V](config)(fromBytesK.mapK(gf), fromBytesV.mapK(gf)).mapK(fg)
        } yield {
          a.mapK(fg, gf)
        }
      }
    }
  }
}
