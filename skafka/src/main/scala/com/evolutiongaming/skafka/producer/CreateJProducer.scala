package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.Bytes
import org.apache.kafka.clients.producer.{KafkaProducer, Producer => ProducerJ}
import org.apache.kafka.common.serialization.ByteArraySerializer


object CreateJProducer {

  def apply(config: ProducerConfig): ProducerJ[Bytes, Bytes] = {
    val properties = config.properties
    val serializer = new ByteArraySerializer()
    new KafkaProducer[Bytes, Bytes](properties, serializer, serializer)
  }
}
