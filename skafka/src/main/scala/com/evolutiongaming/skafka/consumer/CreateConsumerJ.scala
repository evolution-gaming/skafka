package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.{Blocking, FromBytes}
import org.apache.kafka.clients.consumer.KafkaConsumer

object CreateConsumerJ {

  def apply[F[_] : ToTry, K, V](
    config: ConsumerConfig,
    FromBytesK: FromBytes[F, K],
    fromBytesV: FromBytes[F, V],
    blocking: Blocking[F]
  ): F[KafkaConsumer[K, V]] = {

    val deserializerK = fromBytesV.asJava
    val deserializerV = FromBytesK.asJava
    blocking { new KafkaConsumer(config.properties, deserializerV, deserializerK) }
  }
}
