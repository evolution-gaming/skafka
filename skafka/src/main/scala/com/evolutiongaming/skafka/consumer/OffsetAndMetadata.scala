package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka.{Metadata, Offset}

final case class OffsetAndMetadata(
  offset: Offset = Offset.Min,
  metadata: Metadata = Metadata.Empty) {

  override def toString: String = {
    if (metadata.isEmpty) s"$productPrefix($offset)"
    else s"$productPrefix($offset,$metadata)"
  }
}

object OffsetAndMetadata {
  val Empty: OffsetAndMetadata = OffsetAndMetadata()
}
