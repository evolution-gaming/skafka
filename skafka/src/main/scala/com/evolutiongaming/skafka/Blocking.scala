package com.evolutiongaming.skafka

import cats.effect.{Sync, ContextShift}
import com.evolutiongaming.catshelper.FromFuture

import scala.concurrent.{ExecutionContext, Future}

trait Blocking[F[_]] {
  def apply[A](thunk: => A): F[A]

  def future[A](f: => Future[A]): F[A]
}

object Blocking {
  def apply[F[_] : Sync : ContextShift : FromFuture](ec: ExecutionContext): Blocking[F] =
    new Blocking[F] {
      def blocking[A](f: F[A]): F[A] = ContextShift[F].evalOn(ec)(f)

      override def apply[A](thunk: => A): F[A] = blocking {
        Sync[F].delay(thunk)
      }

      override def future[A](f: => Future[A]): F[A] = blocking {
        val ff = FromFuture[F]
        ff(f)
      }
    }
}