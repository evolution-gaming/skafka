package com.evolutiongaming.skafka.consumer

sealed trait IsolationLevel extends Product {
  def name: String
}

object IsolationLevel {
  val Values: Set[IsolationLevel] = Set(ReadCommitted, ReadUncommitted)

  case object ReadCommitted extends IsolationLevel { def name: String = "read_committed" }
  case object ReadUncommitted extends IsolationLevel { def name: String = "read_uncommitted" }
}
