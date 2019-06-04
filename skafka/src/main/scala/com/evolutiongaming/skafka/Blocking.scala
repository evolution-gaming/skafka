package com.evolutiongaming.skafka

import cats.effect.{ContextShift, Sync}

import scala.concurrent.ExecutionContext

trait Blocking[F[_]] {

  def apply[A](f: => A): F[A]
}

object Blocking {

  def apply[F[_]](implicit F: Blocking[F]): Blocking[F] = F


  def apply[F[_] : Sync : ContextShift](executor: ExecutionContext): Blocking[F] = {

    new Blocking[F] {

      def apply[A](a: => A) = {
        ContextShift[F].evalOn(executor) { Sync[F].delay { a } }
      }
    }
  }
}