package com.evolutiongaming.skafka.consumer

import java.time.Instant

import com.evolutiongaming.skafka.Offset

case class OffsetAndTimestamp(offset: Offset, timestamp: Instant)
