package com.evolutiongaming.skafka

final case class OffsetAndMetadata(
  offset: Offset = Offset.Min,
  metadata: Metadata = Metadata.Empty) {

  override def toString = {
    if (metadata.isEmpty) s"$productPrefix($offset)"
    else s"$productPrefix($offset,$metadata)"
  }
}

object OffsetAndMetadata {
  val Empty: OffsetAndMetadata = OffsetAndMetadata()
}
