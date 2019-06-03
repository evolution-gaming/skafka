package com.evolutiongaming.skafka

import cats.effect.{ContextShift, Sync}

import scala.concurrent.ExecutionContext

trait Blocking[F[_]] {

  def apply[A](f: => A): F[A]
}

object Blocking {

  def apply[F[_]](implicit F: Blocking[F]): Blocking[F] = F


  def apply[F[_] : Sync : ContextShift](executor: ExecutionContext): Blocking[F] = {

    def blocking[A](fa: F[A]) = ContextShift[F].evalOn(executor)(fa)

    new Blocking[F] {

      def apply[A](a: => A) = {
        blocking { Sync[F].delay(a) }
      }
    }
  }
}