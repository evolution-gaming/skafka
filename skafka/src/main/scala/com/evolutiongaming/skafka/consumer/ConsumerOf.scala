package com.evolutiongaming.skafka.consumer

import cats.effect.{Concurrent, ContextShift, Resource}
import cats.{Applicative, Defer, ~>}
import com.evolutiongaming.catshelper.{ToFuture, ToTry}
import com.evolutiongaming.skafka.FromBytes
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.ExecutionContext

trait ConsumerOf[F[_]] {

  def apply[K, V](
    config: ConsumerConfig
  )(implicit fromBytesK: FromBytes[F, K], fromBytesV: FromBytes[F, V]): Resource[F, Consumer[F, K, V]]
}

object ConsumerOf {

  def apply[F[_]: Concurrent: ContextShift: ToTry: ToFuture: MeasureDuration](
    executorBlocking: ExecutionContext,
    metrics: Option[ConsumerMetrics[F]] = None
  ): ConsumerOf[F] = new ConsumerOf[F] {

    def apply[K, V](config: ConsumerConfig)(implicit fromBytesK: FromBytes[F, K], fromBytesV: FromBytes[F, V]) = {
      for {
        consumer <- Consumer.of[F, K, V](config, executorBlocking)
      } yield {
        metrics.fold(consumer)(consumer.withMetrics[Throwable])
      }
    }
  }

  implicit class ConsumerOfOps[F[_]](val self: ConsumerOf[F]) extends AnyVal {

    def mapK[G[_]: Applicative: Defer](
      fg: F ~> G,
      gf: G ~> F
    ): ConsumerOf[G] = new ConsumerOf[G] {

      def apply[K, V](config: ConsumerConfig)(implicit fromBytesK: FromBytes[G, K], fromBytesV: FromBytes[G, V]) = {
        for {
          a <- self[K, V](config)(fromBytesK.mapK(gf), fromBytesV.mapK(gf)).mapK(fg)
        } yield {
          a.mapK(fg, gf)
        }
      }
    }
  }
}
