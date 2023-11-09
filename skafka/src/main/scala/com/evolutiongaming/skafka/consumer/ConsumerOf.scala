package com.evolutiongaming.skafka.consumer

import cats.effect._
import cats.~>
import com.evolutiongaming.catshelper.{MeasureDuration, ToFuture, ToTry}
import com.evolutiongaming.skafka.FromBytes

import scala.concurrent.ExecutionContext

trait ConsumerOf[F[_]] {

  def apply[K, V](
    config: ConsumerConfig
  )(implicit fromBytesK: FromBytes[F, K], fromBytesV: FromBytes[F, V]): Resource[F, Consumer[F, K, V]]
}

object ConsumerOf {

  @deprecated("Use apply1", since = "12.0.1")
  def apply[F[_]: Async: ToTry: ToFuture: MeasureDuration](
    executorBlocking: ExecutionContext,
    metrics: Option[ConsumerMetrics[F]] = None
  ): ConsumerOf[F] = apply1(metrics)

  def apply1[F[_]: Async: ToTry: ToFuture: MeasureDuration](
    metrics: Option[ConsumerMetrics[F]] = None
  ): ConsumerOf[F] = {
    class Main
    new Main with ConsumerOf[F] {

      def apply[K, V](config: ConsumerConfig)(implicit fromBytesK: FromBytes[F, K], fromBytesV: FromBytes[F, V]) = {
        Consumer
          .of[F, K, V](config)
          .map { consumer =>
            metrics.fold { consumer } { consumer.withMetrics1[Throwable] }
          }
      }
    }
  }

  /** The sole purpose of this method is to support binary compatibility with an intermediate
   *  version (namely, 15.2.0) which had `apply1` method using `MeasureDuration` from `smetrics`
   *  and `apply2` using `MeasureDuration` from `cats-helper`.
   *  This should not be used and should be removed in a reasonable amount of time.
   */
  @deprecated("Use `apply1`", since = "16.0.3")
  def apply2[F[_] : Async : ToTry : ToFuture : MeasureDuration](
    metrics: Option[ConsumerMetrics[F]] = None
  ): ConsumerOf[F] = apply1(metrics)

  implicit class ConsumerOfOps[F[_]](val self: ConsumerOf[F]) extends AnyVal {

    def mapK[G[_]](
      fg: F ~> G,
      gf: G ~> F
    )(implicit F: MonadCancel[F, _], G: MonadCancel[G, _]): ConsumerOf[G] = new ConsumerOf[G] {

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
