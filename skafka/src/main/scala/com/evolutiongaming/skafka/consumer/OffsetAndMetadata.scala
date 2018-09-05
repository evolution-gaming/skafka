package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka.{Metadata, Offset}

final case class OffsetAndMetadata(offset: Offset, metadata: Metadata)
