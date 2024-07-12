package com.evolutiongaming.skafka.producer

import cats.effect.{Resource, Sync}
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.metrics.KafkaMetricsRegistry
import io.prometheus.client.CollectorRegistry

import scala.concurrent.duration.FiniteDuration

object ProducerMetricsOf {

  /**
    * Construct [[ProducerMetrics]] that will expose Java Kafka client metrics.
    *
    * @param source original [[ProducerMetrics]]
    * @param prefix metric name prefix
    * @param prometheus instance of Prometheus registry
    * @return [[ProducerMetrics]] that exposes Java Kafka client metrics
    */
  def withJavaClientMetrics[F[_]: Sync: ToTry](
    source: ProducerMetrics[F],
    prefix: Option[String],
    prometheus: CollectorRegistry
  ): Resource[F, ProducerMetrics[F]] =
    for {
      registry <- KafkaMetricsRegistry.of(prometheus, prefix)
    } yield new ProducerMetrics[F] {
      override def initTransactions(latency: FiniteDuration): F[Unit] = source.initTransactions(latency)

      override def beginTransaction: F[Unit] = source.beginTransaction

      override def sendOffsetsToTransaction(latency: FiniteDuration): F[Unit] = source.sendOffsetsToTransaction(latency)

      override def commitTransaction(latency: FiniteDuration): F[Unit] = source.commitTransaction(latency)

      override def abortTransaction(latency: FiniteDuration): F[Unit] = source.abortTransaction(latency)

      override def send(topic: Topic, latency: FiniteDuration, bytes: Int): F[Unit] = source.send(topic, latency, bytes)

      override def block(topic: Topic, latency: FiniteDuration): F[Unit] = source.block(topic, latency)

      override def failure(topic: Topic, latency: FiniteDuration): F[Unit] = source.failure(topic, latency)

      override def partitions(topic: Topic, latency: FiniteDuration): F[Unit] = source.partitions(topic, latency)

      override def flush(latency: FiniteDuration): F[Unit] = source.flush(latency)

      override def exposeJavaMetrics(producer: Producer[F]): Resource[F, Unit] =
        registry.register(producer.clientMetrics)

    }

  implicit final class ProducerMetricsOps[F[_]](val source: ProducerMetrics[F]) extends AnyVal {

    /**
      * Construct [[ProducerMetrics]] that will expose Java Kafka client metrics.
      *
      * @param prefix metric name prefix
      * @param prometheus instance of Prometheus registry
      * @return [[ProducerMetrics]] that exposes Java Kafka client metrics
      */
    def exposeJavaClientMetrics(
      prefix: Option[String],
      prometheus: CollectorRegistry
    )(implicit F: Sync[F], toTry: ToTry[F]): Resource[F, ProducerMetrics[F]] =
      withJavaClientMetrics(source, prefix, prometheus)

  }

}
