package com.evolutiongaming.skafka.consumer

sealed trait GroupProtocol extends Product {
  def name: String
}

object GroupProtocol {
  val Values: Set[GroupProtocol] = Set(Classic, Consumer)

  case object Classic extends GroupProtocol { def name: String = "classic" }
  case object Consumer extends GroupProtocol { def name: String = "consumer" }
}
