package com.evolutiongaming.skafka

import cats.Show
import cats.syntax.all._
import cats.kernel.Order
import com.evolutiongaming.catshelper.ApplicativeThrowable

import scala.util.Try

sealed abstract case class Partition(value: Int) {

  override def toString: String = value.toString
}

object Partition {

  val min: Partition = new Partition(0) {}

  val max: Partition = new Partition(Int.MaxValue) {}


  implicit val showPartition: Show[Partition] = Show.fromToString


  implicit val orderingPartition: Ordering[Partition] = (x: Partition, y: Partition) => x.value compare y.value

  implicit val orderPartition: Order[Partition] = Order.fromOrdering


  def of[F[_] : ApplicativeThrowable](value: Int): F[Partition] = {
    if (value < min.value) {
      SkafkaError(s"invalid Partition of $value, it must be greater or equal to $min").raiseError[F, Partition]
    } else if (value > max.value) {
      SkafkaError(s"invalid Partition of $value, it must be less or equal to $max").raiseError[F, Partition]
    } else if (value == min.value) {
      min.pure[F]
    } else if (value == max.value) {
      max.pure[F]
    } else {
      new Partition(value) {}.pure[F]
    }
  }


  def unsafe[A](value: A)(implicit numeric: Numeric[A]): Partition = of[Try](numeric.toInt(value)).get
}