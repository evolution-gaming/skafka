package com.evolutiongaming.skafka

import scala.concurrent.{ExecutionContext, Future}

trait Blocking[F[_]] {
  def apply[T](f: => T): F[T]
}

object Blocking {

  def future(ec: ExecutionContext): Blocking[Future] = new Blocking[Future] {
    def apply[T](f: => T): Future[T] = Future(f)(ec)
  }
}
