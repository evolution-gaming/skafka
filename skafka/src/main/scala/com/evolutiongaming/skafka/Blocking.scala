package com.evolutiongaming.skafka

import cats.effect.{Async, ContextShift}
import com.evolutiongaming.catshelper.FromFuture

import scala.concurrent.{ExecutionContext, Future}

final case class Blocking(ec: ExecutionContext)

object Blocking {
  private[skafka] def blocking[F[_] : ContextShift, A](f: F[A])(implicit b: Blocking): F[A] =
    ContextShift[F].evalOn(b.ec)(f)

  private[skafka] def blocking[F[_] : Async : ContextShift, A](thunk: => A)(implicit b: Blocking): F[A] =
    blocking {
      Async[F].delay(thunk)
    }

  private[skafka] def blockingS[F[_] : Async : ContextShift, A](thunk: => A)(implicit b: Blocking): F[A] =
    blocking(thunk)

  private[skafka] def fromFutureBlocking[F[_] : Async : ContextShift, A](f: => Future[A])(implicit b: Blocking): F[A] =
    blocking {
      FromFuture.lift(Async[F], b.ec)(f)
    }
}
