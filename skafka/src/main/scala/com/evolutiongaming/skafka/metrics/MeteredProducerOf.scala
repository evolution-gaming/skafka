package com.evolutiongaming.skafka.metrics

import cats.effect.Resource
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}

class MeteredProducerOf[F[_]](producerOf: ProducerOf[F], kafkaMetricsRegistry: KafkaMetricsRegistry[F])
    extends ProducerOf[F] {
  override def apply(config: ProducerConfig): Resource[F, Producer[F]] = {
    for {
      producer <- producerOf.apply(config)
      _        <- kafkaMetricsRegistry.register(producer.clientMetrics)
    } yield producer
  }
}

object MeteredProducerOf {
  def wrap[F[_]](producerOf: ProducerOf[F], kafkaMetricsRegistry: KafkaMetricsRegistry[F]): MeteredProducerOf[F] =
    new MeteredProducerOf[F](producerOf, kafkaMetricsRegistry)
}
