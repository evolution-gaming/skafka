package com.evolutiongaming.skafka

import cats.Show
import cats.implicits.*
import cats.kernel.Order
import com.evolutiongaming.catshelper.ApplicativeThrowable

sealed abstract case class Offset(value: Long) {

  override def toString: String = value.toString
}

object Offset {

  val min: Offset = new Offset(0) {}

  val max: Offset = new Offset(Long.MaxValue) {}

  implicit val showOffset: Show[Offset] = Show.fromToString

  implicit val orderingOffset: Ordering[Offset] = (x: Offset, y: Offset) => x.value compare y.value

  implicit val orderOffset: Order[Offset] = Order.fromOrdering

  def of[F[_]: ApplicativeThrowable](value: Long): F[Offset] = {
    ApplicativeThrowable[F].catchNonFatal { unsafeOf(value) }
  }

  def unsafe[A](value: A)(implicit numeric: Numeric[A]): Offset = unsafeOf(numeric.toLong(value))

  /** Throws [[SkafkaError]] if `value` is out of range.
    */
  private[skafka] def unsafeOf(value: Long): Offset = {
    if (value < min.value) {
      throw SkafkaError(s"invalid Offset of $value, it must be greater or equal to $min")
    } else if (value > max.value) {
      throw SkafkaError(s"invalid Offset of $value, it must be less or equal to $max")
    } else if (value == min.value) {
      min
    } else if (value == max.value) {
      max
    } else {
      new Offset(value) {}
    }
  }
}
