package com.evolutiongaming.skafka.consumer

import cats.effect.{Clock, Concurrent, ContextShift, Resource}
import cats.{Applicative, Defer, Monad, ~>}
import com.evolutiongaming.catshelper.{MeasureDuration, ToFuture, ToTry}
import com.evolutiongaming.skafka.FromBytes
import com.evolutiongaming.smetrics

import scala.concurrent.ExecutionContext

trait ConsumerOf[F[_]] {

  def apply[K, V](
    config: ConsumerConfig
  )(implicit fromBytesK: FromBytes[F, K], fromBytesV: FromBytes[F, V]): Resource[F, Consumer[F, K, V]]
}

object ConsumerOf {

  @deprecated("use `apply1` instead", "11.13.0")
  def apply[F[_]: Concurrent: ContextShift: ToTry: ToFuture: smetrics.MeasureDuration](
    executorBlocking: ExecutionContext,
    metrics: Option[ConsumerMetrics[F]] = None
  ): ConsumerOf[F] = {
    class Main
    new Main with ConsumerOf[F] {

      def apply[K, V](config: ConsumerConfig)(implicit fromBytesK: FromBytes[F, K], fromBytesV: FromBytes[F, V]) = {
        for {
          consumer <- Consumer.of[F, K, V](config, executorBlocking)
        } yield {
          metrics.fold(consumer)(consumer.withMetrics[Throwable])
        }
      }
    }
  }

  @deprecated("Use apply2", since = "11.15.1")
  def apply1[F[_]: Concurrent: ContextShift: ToTry: ToFuture: smetrics.MeasureDuration: Clock](
    executorBlocking: ExecutionContext,
    metrics: Option[ConsumerMetrics[F]] = None
  ): ConsumerOf[F] = {
    implicit val md: MeasureDuration[F] = smetrics.MeasureDuration[F].toCatsHelper
    apply2(executorBlocking, metrics)
  }

  def apply2[F[_]: Concurrent: ContextShift: ToTry: ToFuture: MeasureDuration: Clock](
    executorBlocking: ExecutionContext,
    metrics: Option[ConsumerMetrics[F]] = None
  ): ConsumerOf[F] = {
    class Main
    new Main with ConsumerOf[F] {

      def apply[K, V](config: ConsumerConfig)(implicit fromBytesK: FromBytes[F, K], fromBytesV: FromBytes[F, V]) = {
        Consumer
          .of[F, K, V](config, executorBlocking)
          .map { consumer =>
            metrics.fold { consumer } { consumer.withMetrics2[Throwable] }
          }
      }
    }
  }

  implicit class ConsumerOfOps[F[_]](val self: ConsumerOf[F]) extends AnyVal {

    def mapK[G[_]: Applicative: Defer](
      fg: F ~> G,
      gf: G ~> F
    )(implicit F: Monad[F]): ConsumerOf[G] = new ConsumerOf[G] {

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
