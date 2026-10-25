package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.config.ConfigHelper.*
import com.evolutiongaming.skafka.ConfigHelpers
import com.typesafe.config.Config

sealed trait GroupProtocol extends Product {
  def name: String
}

object GroupProtocol {

  case object Classic extends GroupProtocol { def name: String = "classic" }

  /** @param remoteAssignor
    *   `group.remote.assignor`, the server-side assignor; the broker default is used if not set
    */
  final case class Consumer(remoteAssignor: Option[String] = None) extends GroupProtocol {
    def name: String = "consumer"
  }

  // matches by name only, so `Consumer()` stands in for any assignor
  private implicit val NameFromConf: FromConf[GroupProtocol] =
    ConfigHelpers.enumFromConf(Set[GroupProtocol](Classic, Consumer()), "GroupProtocol")(_.name)

  /** Reads `group-protocol` and, under `consumer`, `group-remote-assignor`; what is not set comes from `default` */
  def apply(config: Config, default: => GroupProtocol): GroupProtocol = {
    config.getOpt[GroupProtocol]("group-protocol", "group.protocol") getOrElse default match {
      case Classic     => Classic
      case Consumer(_) =>
        val defaultAssignor = default match {
          case Consumer(assignor) => assignor
          case Classic            => None
        }
        Consumer(config.getOpt[String]("group-remote-assignor", "group.remote.assignor") orElse defaultAssignor)
    }
  }
}
