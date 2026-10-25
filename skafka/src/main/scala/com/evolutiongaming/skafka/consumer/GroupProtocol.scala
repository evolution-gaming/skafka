package com.evolutiongaming.skafka.consumer

sealed trait GroupProtocol extends Product {
  def name: String
}

object GroupProtocol {
  // the protocols by name, for parsing; `Consumer()` stands in for any assignor
  private[skafka] val Values: Set[GroupProtocol] = Set(Classic, Consumer())

  case object Classic extends GroupProtocol { def name: String = "classic" }

  /** @param remoteAssignor
    *   `group.remote.assignor`, the server-side assignor; the broker default is used if not set
    */
  final case class Consumer(remoteAssignor: Option[String] = None) extends GroupProtocol {
    def name: String = "consumer"
  }
}
