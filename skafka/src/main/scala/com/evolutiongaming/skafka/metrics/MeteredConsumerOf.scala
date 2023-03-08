package com.evolutiongaming.skafka.metrics

import cats.effect.Resource
import com.evolutiongaming.skafka.FromBytes
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig, ConsumerOf}

class MeteredConsumerOf[F[_]](consumerOf: ConsumerOf[F], kafkaMetricsRegistry: KafkaMetricsRegistry[F])
    extends ConsumerOf[F] {

  override def apply[K, V](
    config: ConsumerConfig
  )(implicit fromBytesK: FromBytes[F, K], fromBytesV: FromBytes[F, V]): Resource[F, Consumer[F, K, V]] = {
    for {
      consumer <- consumerOf.apply[K, V](config)
      _        <- kafkaMetricsRegistry.register(consumer.clientMetrics)
    } yield consumer
  }
}

object MeteredConsumerOf {
  def wrap[F[_]](consumerOf: ConsumerOf[F], kafkaMetricsRegistry: KafkaMetricsRegistry[F]): MeteredConsumerOf[F] =
    new MeteredConsumerOf[F](consumerOf, kafkaMetricsRegistry)
}
