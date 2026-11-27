package com.evolutiongaming.skafka

import cats.{Eq, Show}
import cats.implicits.*
import cats.kernel.Order
import com.evolutiongaming.catshelper.ApplicativeThrowable

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
    ApplicativeThrowable[F].catchNonFatal { unsafeOf(value) }
  }

  def unsafe[A](value: A)(implicit numeric: Numeric[A]): Partition = unsafeOf(numeric.toInt(value))

  /** Throws [[SkafkaError]] if `value` is out of range.
    */
  private[skafka] def unsafeOf(value: Int): Partition = {
    if (value < min.value) {
      throw SkafkaError(s"invalid Partition of $value, it must be greater or equal to $min")
    } else if (value > max.value) {
      throw SkafkaError(s"invalid Partition of $value, it must be less or equal to $max")
    } else if (value == min.value) {
      min
    } else if (value == max.value) {
      max
    } else {
      new Impl(value)
    }
  }
}
