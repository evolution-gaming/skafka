package com.evolutiongaming.skafka.producer

import cats.effect.Sync
import com.evolutiongaming.skafka.Bytes
import org.apache.kafka.clients.producer.{KafkaProducer, Producer => ProducerJ}
import org.apache.kafka.common.serialization.ByteArraySerializer


object CreateProducerJ {

  def apply[F[_] : Sync](config: ProducerConfig): F[ProducerJ[Bytes, Bytes]] = {
    val properties = config.properties
    val serializer = new ByteArraySerializer()
    Sync[F].delay { new KafkaProducer[Bytes, Bytes](properties, serializer, serializer) }
  }
}