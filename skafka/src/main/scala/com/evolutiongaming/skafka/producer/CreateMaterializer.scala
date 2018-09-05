package com.evolutiongaming.skafka.producer

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}

object CreateMaterializer {

  def apply(configs: ProducerConfig)(implicit system: ActorSystem): Materializer = {
    val namePrefix = configs.common.clientId match {
      case None           => "skafka"
      case Some(clientId) => s"skafka-$clientId"
    }
    ActorMaterializer(namePrefix = Some(namePrefix))
  }
}
