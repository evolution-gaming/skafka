package com.evolutiongaming.skafka.consumer

import cats.effect.Sync
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.FromBytes
import org.apache.kafka.clients.consumer.KafkaConsumer

object CreateConsumerJ {

  def apply[F[_] : Sync, K, V](
    config: ConsumerConfig,
    valueFromBytes: FromBytes[V],
    keyFromBytes: FromBytes[K]
  ): F[KafkaConsumer[K, V]] = {

    val valueDeserializer = valueFromBytes.asJava
    val keyDeserializer = keyFromBytes.asJava
    val consumer = new KafkaConsumer(config.properties, keyDeserializer, valueDeserializer)
    Sync[F].delay { consumer }
  }
}
