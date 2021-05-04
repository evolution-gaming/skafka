package com.evolutiongaming.skafka

import java.time.Instant

final case class TimestampAndType(timestamp: Instant, timestampType: TimestampType)

sealed trait TimestampType

object TimestampType {
  case object Create extends TimestampType
  case object Append extends TimestampType
}
