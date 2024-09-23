package com.evolutiongaming.skafka.consumer

import cats.effect.{Resource, Sync}
import cats.effect.std.UUIDGen
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.{ClientId, Topic, TopicPartition}
import com.evolutiongaming.skafka.metrics.KafkaMetricsRegistry
import io.prometheus.client.CollectorRegistry

import scala.concurrent.duration.FiniteDuration

object ConsumerMetricsOf {

  /**
    * Construct [[ConsumerMetrics]] that will expose Java Kafka client metrics.
    *
    * @param sourceOf original [[ConsumerMetrics]] factory
    * @param prometheus instance of Prometheus registry
    * @param prefix metric name prefix
    * @return [[ConsumerMetrics]] that exposes Java Kafka client metrics
    */
  def withJavaClientMetrics[F[_]: Sync: ToTry: UUIDGen](
    sourceOf: ClientId => ConsumerMetrics[F],
    prometheus: CollectorRegistry,
    prefix: Option[String],
  ): Resource[F, ClientId => ConsumerMetrics[F]] =
    for {
      registry <- KafkaMetricsRegistry.of(prometheus, prefix)
    } yield { (clientId: ClientId) =>
      val source = sourceOf(clientId)
      consumerMetricsOf(source, registry)
    }

  /**
    * Construct [[ConsumerMetrics]] that will expose Java Kafka client metrics.
    *
    * @param source original [[ConsumerMetrics]]
    * @param prometheus instance of Prometheus registry
    * @param prefix metric name prefix
    * @return [[ConsumerMetrics]] that exposes Java Kafka client metrics
    */
  def withJavaClientMetrics[F[_]: Sync: ToTry: UUIDGen](
    source: ConsumerMetrics[F],
    prometheus: CollectorRegistry,
    prefix: Option[String],
  ): Resource[F, ConsumerMetrics[F]] =
    for {
      registry <- KafkaMetricsRegistry.of(prometheus, prefix)
    } yield consumerMetricsOf(source, registry)

  private def consumerMetricsOf[F[_]](
    source: ConsumerMetrics[F],
    registry: KafkaMetricsRegistry[F],
  ): ConsumerMetrics[F] = new WithJavaClientMetrics(source, registry)

  implicit final class ConsumerMetricsOps[F[_]](val source: ConsumerMetrics[F]) extends AnyVal {

    /**
      * Construct [[ConsumerMetrics]] that will expose Java Kafka client metrics.
      *
      * @param prometheus instance of Prometheus registry
      * @param prefix metric name prefix
      * @return [[ConsumerMetrics]] that exposes Java Kafka client metrics
      */
    def exposeJavaClientMetrics(
      prometheus: CollectorRegistry,
      prefix: Option[String] = None,
    )(implicit F: Sync[F], toTry: ToTry[F]): Resource[F, ConsumerMetrics[F]] =
      withJavaClientMetrics(source, prometheus, prefix)

  }

  private final class WithJavaClientMetrics[F[_]](source: ConsumerMetrics[F], registry: KafkaMetricsRegistry[F])
      extends ConsumerMetrics[F] {
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
}
