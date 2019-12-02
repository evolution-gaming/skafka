package com.evolutiongaming.skafka

final case class OffsetAndMetadata(
  offset: Offset = Offset.min,
  metadata: Metadata = Metadata.empty) {

  override def toString = {
    if (metadata.isEmpty) s"$productPrefix($offset)"
    else s"$productPrefix($offset,$metadata)"
  }
}

object OffsetAndMetadata {
  val empty: OffsetAndMetadata = OffsetAndMetadata()
}
