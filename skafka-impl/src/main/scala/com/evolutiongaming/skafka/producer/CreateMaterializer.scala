package com.evolutiongaming.skafka.producer

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}

object CreateMaterializer {

  def apply(configs: Configs)(implicit system: ActorSystem): Materializer = {
    val namePrefix = configs.clientId match {
      case None           => "skafka"
      case Some(clientId) => s"skafka-$clientId"
    }
    ActorMaterializer(namePrefix = Some(namePrefix))
  }
}
