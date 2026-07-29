package com.evolutiongaming.skafka.metrics

import cats.Monad
import cats.effect.Resource
import com.evolutiongaming.skafka.ClientId
import com.evolutiongaming.skafka.consumer.ConsumerMetrics
import com.evolutiongaming.skafka.producer.ProducerMetrics
import com.evolutiongaming.smetrics.CollectorRegistry

trait KafkaMetrics[F[_]] {

  def consumer(clientId: ClientId): ConsumerMetrics[F]

  def producer(clientId: ClientId): ProducerMetrics[F]
}

object KafkaMetrics {

  def make[F[_]: Monad](registry: CollectorRegistry[F]): Resource[F, KafkaMetrics[F]] = {
    for {
      consumerMetrics <- ConsumerMetrics.of[F](registry)
      producerMetrics <- ProducerMetrics.of[F](registry)
    } yield {
      new KafkaMetrics[F] {

        def consumer(clientId: ClientId): ConsumerMetrics[F] = consumerMetrics(clientId)

        def producer(clientId: ClientId): ProducerMetrics[F] = producerMetrics(clientId)
      }
    }
  }
}
