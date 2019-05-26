package com.evolutiongaming

import cats.effect.Async
import cats.{Id, ~>}

package object skafka {

  type ClientId = String

  type Partition = Int

  object Partition {
    val Min: Partition = 0
  }


  type Offset = Long

  object Offset {
    val Min: Offset = 0l
  }


  type Topic = String


  type Metadata = String

  object Metadata {
    val Empty: Metadata = ""
  }


  type Bytes = Array[Byte]

  object Bytes {
    val Empty: Bytes = Array.empty
  }

  private[skafka] implicit class NaturalTransformation[F[_], A](fa: F[A]) {
    def liftTo[G[_]](implicit transform: F ~> G): G[A] =
      transform(fa)
  }

  private[skafka] implicit def selfArrow[F[_]]: F ~> F = new (F ~> F) {
    override def apply[A](fa: F[A]): F[A] = fa
  }

  private[skafka] implicit def delayArrow[F[_] : Async] = new (Id ~> F) {
    override def apply[A](fa: Id[A]): F[A] = Async[F].delay(fa)
  }
}