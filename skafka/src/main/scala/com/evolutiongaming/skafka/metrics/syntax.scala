package com.evolutiongaming.skafka.metrics

import com.evolutiongaming.skafka.consumer.ConsumerOf
import com.evolutiongaming.skafka.producer.ProducerOf

object syntax {
  implicit final class MeteredProducerOfOps[F[_]](val producerOf: ProducerOf[F]) extends AnyVal {
    def withNativeMetrics(registry: KafkaMetricsRegistry[F]): ProducerOf[F] = MeteredProducerOf.wrap(producerOf, registry)
  }

  implicit final class MeteredConsumerOfOps[F[_]](val consumerOf: ConsumerOf[F]) extends AnyVal {
    def withNativeMetrics(registry: KafkaMetricsRegistry[F]): ConsumerOf[F] = MeteredConsumerOf.wrap(consumerOf, registry)
  }
}
