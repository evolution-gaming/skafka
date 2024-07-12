package com.evolutiongaming.skafka.consumer

import cats.effect.{Resource, Sync}
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.{Topic, TopicPartition}
import com.evolutiongaming.skafka.metrics.KafkaMetricsRegistry
import io.prometheus.client.CollectorRegistry

import scala.concurrent.duration.FiniteDuration

object ConsumerMetricsOf {

  /**
    * Construct [[ConsumerMetrics]] that will expose Java Kafka client metrics.
    *
    * @param source original [[ConsumerMetrics]]
    * @param prefix metric name prefix
    * @param prometheus instance of Prometheus registry
    * @return [[ConsumerMetrics]] that exposes Java Kafka client metrics
    */
  def withJavaClientMetrics[F[_]: Sync: ToTry](
    source: ConsumerMetrics[F],
    prefix: Option[String],
    prometheus: CollectorRegistry
  ): Resource[F, ConsumerMetrics[F]] =
    for {
      registry <- KafkaMetricsRegistry.of(prometheus, prefix)
    } yield new ConsumerMetrics[F] {
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

      override def exposeJavaMetrics[K, V](consumer: Consumer[F, K, V]): Resource[F, Unit] =
        registry.register(consumer.clientMetrics)

    }

  implicit final class ConsumerMetricsOps[F[_]](val source: ConsumerMetrics[F]) extends AnyVal {

    /**
      * Construct [[ConsumerMetrics]] that will expose Java Kafka client metrics.
      *
      * @param prefix function that provides _unique_ prefix for each client
      * @param prometheus instance of Prometheus registry
      * @return [[ConsumerMetrics]] that exposes Java Kafka client metrics
      */
    def exposeJavaClientMetrics(
      prefix: Option[String],
      prometheus: CollectorRegistry
    )(implicit F: Sync[F], toTry: ToTry[F]): Resource[F, ConsumerMetrics[F]] =
      withJavaClientMetrics(source, prefix, prometheus)

  }
}
