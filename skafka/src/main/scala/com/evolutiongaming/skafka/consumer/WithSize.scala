package com.evolutiongaming.skafka.consumer

import cats.Order

final case class WithSize[+A](value: A, serializedSize: Int = 0)

object WithSize {
  implicit def orderWithSize[A: Order]: Order[WithSize[A]] = Order.by { _.value }
}
