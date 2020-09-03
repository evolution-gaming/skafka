package com.evolutiongaming.skafka

import cats.Show
import cats.syntax.all._
import cats.kernel.Order
import com.evolutiongaming.catshelper.ApplicativeThrowable

import scala.util.Try

sealed abstract case class Offset(value: Long) {

  override def toString: String = value.toString
}

object Offset {

  val min: Offset = new Offset(0) {}

  val max: Offset = new Offset(Long.MaxValue) {}


  implicit val showOffset: Show[Offset] = Show.fromToString


  implicit val orderingOffset: Ordering[Offset] = (x: Offset, y: Offset) => x.value compare y.value

  implicit val orderOffset: Order[Offset] = Order.fromOrdering


  def of[F[_] : ApplicativeThrowable](value: Long): F[Offset] = {
    if (value < min.value) {
      SkafkaError(s"invalid Offset of $value, it must be greater or equal to $min").raiseError[F, Offset]
    } else if (value > max.value) {
      SkafkaError(s"invalid Offset of $value, it must be less or equal to $max").raiseError[F, Offset]
    } else if (value == min.value) {
      min.pure[F]
    } else if (value == max.value) {
      max.pure[F]
    } else {
      new Offset(value) {}.pure[F]
    }
  }


  def unsafe[A](value: A)(implicit numeric: Numeric[A]): Offset = of[Try](numeric.toLong(value)).get
}
