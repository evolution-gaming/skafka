package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.Bytes
import org.apache.kafka.clients.producer.{KafkaProducer, Producer => JProducer}
import org.apache.kafka.common.serialization.ByteArraySerializer


object CreateJProducer {

  def apply(configs: ProducerConfig): JProducer[Bytes, Bytes] = {
    val properties = configs.properties
    val serializer = new ByteArraySerializer()
    new KafkaProducer[Bytes, Bytes](properties, serializer, serializer)
  }
}
