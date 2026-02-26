package com.evolutiongaming.skafka

import java.time.Instant

final case class TimestampAndType(timestamp: Instant, timestampType: TimestampType)

sealed trait TimestampType

object TimestampType {

  def create: TimestampType = Create

  def append: TimestampType = Append

  case object Create extends TimestampType

  case object Append extends TimestampType
}
