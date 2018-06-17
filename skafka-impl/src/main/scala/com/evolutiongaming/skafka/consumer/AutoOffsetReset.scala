package com.evolutiongaming.skafka.consumer

sealed trait AutoOffsetReset extends Product

object AutoOffsetReset {

  val Values: Set[AutoOffsetReset] = Set(Latest, Earliest, None)

  case object Latest extends AutoOffsetReset
  case object Earliest extends AutoOffsetReset
  case object None extends AutoOffsetReset
}
