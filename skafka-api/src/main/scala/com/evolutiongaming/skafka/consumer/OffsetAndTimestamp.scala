package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka.{Offset, Timestamp}

case class OffsetAndTimestamp(offset: Offset, timestamp: Timestamp)
