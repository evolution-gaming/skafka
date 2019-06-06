package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.{Blocking, Bytes}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer => ProducerJ}
import org.apache.kafka.common.serialization.ByteArraySerializer


object CreateProducerJ {

  def apply[F[_]](
    config: ProducerConfig,
    blocking: Blocking[F]
  ): F[ProducerJ[Bytes, Bytes]] = {
    val properties = config.properties
    val serializer = new ByteArraySerializer()
    blocking { new KafkaProducer[Bytes, Bytes](properties, serializer, serializer) }
  }
}