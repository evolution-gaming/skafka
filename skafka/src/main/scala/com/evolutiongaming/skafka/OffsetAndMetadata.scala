package com.evolutiongaming.skafka

import cats.Show
import cats.implicits._
import cats.kernel.Order

final case class OffsetAndMetadata(offset: Offset = Offset.min, metadata: Metadata = Metadata.empty) {

  override def toString = {
    if (metadata.isEmpty) s"$productPrefix($offset)"
    else s"$productPrefix($offset,$metadata)"
  }
}

object OffsetAndMetadata {

  val empty: OffsetAndMetadata = OffsetAndMetadata()

  implicit val showOffset: Show[OffsetAndMetadata] = Show.fromToString

  implicit val orderOffset: Order[OffsetAndMetadata] =
    Order.whenEqual(Order.by { a: OffsetAndMetadata => a.offset }, Order.by { a: OffsetAndMetadata => a.metadata })

  implicit val orderingOffset: Ordering[OffsetAndMetadata] = orderOffset.toOrdering
}
