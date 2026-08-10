package com.evolutiongaming.skafka

import cats.{Eq, Show}
import cats.implicits.*
import cats.kernel.Order
import com.evolutiongaming.catshelper.ApplicativeThrowable

import scala.util.Try

sealed abstract case class Partition(value: Int) {

  override def toString: String = value.toString
}

object Partition {

  private class Impl(value: Int) extends Partition(value)

  val min: Partition = new Impl(0)

  val max: Partition = new Impl(Int.MaxValue)

  implicit val showPartition: Show[Partition] = Show.fromToString

  implicit val orderingPartition: Ordering[Partition] = (x: Partition, y: Partition) => x.value compare y.value

  implicit val orderPartition: Order[Partition] = Order.fromOrdering

  implicit val eqPartition: Eq[Partition] = Eq.fromUniversalEquals

  def of[F[_]: ApplicativeThrowable](value: Int): F[Partition] = {
    if (value < min.value) {
      SkafkaError(s"invalid Partition of $value, it must be greater or equal to $min").raiseError[F, Partition]
    } else if (value > max.value) {
      SkafkaError(s"invalid Partition of $value, it must be less or equal to $max").raiseError[F, Partition]
    } else if (value == min.value) {
      min.pure[F]
    } else if (value == max.value) {
      max.pure[F]
    } else {
      (new Impl(value): Partition).pure[F]
    }
  }

  def unsafe[A](value: A)(implicit numeric: Numeric[A]): Partition = of[Try](numeric.toInt(value)).get
}
