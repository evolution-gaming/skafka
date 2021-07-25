package com.evolutiongaming.skafka.consumer

import cats.{Functor, Order}

final case class WithSize[+A](value: A, serializedSize: Int = 0)

object WithSize {

  implicit val functorWithSize: Functor[WithSize] = new Functor[WithSize] {
    def map[A, B](fa: WithSize[A])(f: A => B) = {
      fa.copy(f(fa.value))
    }
  }

  implicit def orderWithSize[A: Order]: Order[WithSize[A]] = Order.by { _.value }

  implicit def orderingWithSize[A: Ordering]: Ordering[WithSize[A]] = Ordering.by { _.value }
}
