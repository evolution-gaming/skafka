package com.evolutiongaming.skafka.consumer

import java.time.Instant

import com.evolutiongaming.skafka.Offset

final case class OffsetAndTimestamp(offset: Offset, timestamp: Instant)
