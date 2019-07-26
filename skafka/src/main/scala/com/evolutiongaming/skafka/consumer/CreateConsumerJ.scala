package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.{Blocking, FromBytes}
import org.apache.kafka.clients.consumer.KafkaConsumer

object CreateConsumerJ {

  def apply[F[_] : ToTry, K, V](
    config: ConsumerConfig,
    valueFromBytes: FromBytes[F, V],
    keyFromBytes: FromBytes[F, K],
    blocking: Blocking[F]
  ): F[KafkaConsumer[K, V]] = {

    val valueDeserializer = valueFromBytes.asJava
    val keyDeserializer = keyFromBytes.asJava
    blocking { new KafkaConsumer(config.properties, keyDeserializer, valueDeserializer) }
  }
}
