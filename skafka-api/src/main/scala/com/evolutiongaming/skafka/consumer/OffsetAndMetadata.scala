package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka.{Metadata, Offset}

case class OffsetAndMetadata(offset: Offset, metadata: Metadata)
