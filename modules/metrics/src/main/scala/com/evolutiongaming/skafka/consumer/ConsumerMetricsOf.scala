package com.evolutiongaming.skafka.consumer

import cats.effect.{Resource, Sync}
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.{ClientId, Topic, TopicPartition}
import com.evolutiongaming.skafka.metrics.KafkaMetricsCollector
import io.prometheus.client.CollectorRegistry

import scala.concurrent.duration.FiniteDuration

object ConsumerMetricsOf {

  def withJavaClientMetrics[F[_]: Sync: ToTry](
    source: ConsumerMetrics[F],
    prefix: ClientId => String,
    prometheus: CollectorRegistry
  ): ConsumerMetrics[F] =
    new ConsumerMetrics[F] {
      override def call(name: String, topic: Topic, latency: FiniteDuration, success: Boolean): F[Unit] =
        source.call(name, topic, latency, success)

      override def poll(topic: Topic, bytes: Int, records: Int, age: Option[FiniteDuration]): F[Unit] =
        source.poll(topic, bytes, records, age)

      override def count(name: String, topic: Topic): F[Unit] =
        source.count(name, topic)

      override def rebalance(name: String, topicPartition: TopicPartition): F[Unit] =
        source.rebalance(name, topicPartition)

      override def topics(latency: FiniteDuration): F[Unit] =
        source.topics(latency)

      override def exposeJavaMetrics[K, V](
        consumer: Consumer[F, K, V],
        clientId: Option[ClientId]
      ): Resource[F, Unit] = {
        val collector = new KafkaMetricsCollector[F](consumer.clientMetrics, clientId.map(prefix))
        Resource.make {
          Sync[F].delay { prometheus.register(collector) }
        } { _ =>
          Sync[F].delay { prometheus.unregister(collector) }
        }
      }

    }

  implicit final class Syntax[F[_]](val source: ConsumerMetrics[F]) extends AnyVal {

    def exposeJavaClientMetrics(
      prefix: ClientId => String,
      prometheus: CollectorRegistry
    )(implicit F: Sync[F], toTry: ToTry[F]): ConsumerMetrics[F] = withJavaClientMetrics(source, prefix, prometheus)

  }
}
