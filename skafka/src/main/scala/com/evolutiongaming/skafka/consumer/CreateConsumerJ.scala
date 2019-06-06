package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.{Blocking, FromBytes}
import org.apache.kafka.clients.consumer.KafkaConsumer

object CreateConsumerJ {

  def apply[F[_], K, V](
    config: ConsumerConfig,
    valueFromBytes: FromBytes[V],
    keyFromBytes: FromBytes[K],
    blocking: Blocking[F]
  ): F[KafkaConsumer[K, V]] = {

    val valueDeserializer = valueFromBytes.asJava
    val keyDeserializer = keyFromBytes.asJava
    blocking { new KafkaConsumer(config.properties, keyDeserializer, valueDeserializer) }
  }
}
