package com.evolutiongaming.skafka

import cats.effect.{Async, ContextShift}
import com.evolutiongaming.catshelper.FromFuture

import scala.concurrent.{ExecutionContext, Future}

trait Blocking[F[_]] {
  def apply[A](thunk: => A): F[A]

  def future[A](f: => Future[A]): F[A]
}

object Blocking {
  def apply[F[_] : Async : ContextShift : FromFuture](blockingEC: ExecutionContext): Blocking[F] =
    new Blocking[F] {
      def blocking[A](f: F[A]): F[A] = ContextShift[F].evalOn(blockingEC)(f)

      override def apply[A](thunk: => A): F[A] = blocking {
        Async[F].delay(thunk)
      }

      override def future[A](f: => Future[A]): F[A] = blocking {
        val ff = FromFuture[F]
        ff(f)
      }
    }
}