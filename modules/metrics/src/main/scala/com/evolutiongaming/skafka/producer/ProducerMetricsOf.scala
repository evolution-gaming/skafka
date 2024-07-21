package com.evolutiongaming.skafka.producer

import cats.effect.{Resource, Sync}
import cats.effect.std.UUIDGen
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.{ClientId, Topic}
import com.evolutiongaming.skafka.metrics.KafkaMetricsRegistry
import io.prometheus.client.CollectorRegistry

import scala.concurrent.duration.FiniteDuration

object ProducerMetricsOf {

  /**
    * Construct [[ProducerMetrics]] that will expose Java Kafka client metrics.
    *
    * @param sourceOf original [[ProducerMetrics]] factory
    * @param prometheus instance of Prometheus registry
    * @param prefix metric name prefix
    * @return [[ProducerMetrics]] that exposes Java Kafka client metrics
    */
  def withJavaClientMetrics[F[_]: Sync: ToTry: UUIDGen](
    sourceOf: ClientId => ProducerMetrics[F],
    prometheus: CollectorRegistry,
    prefix: Option[String],
  ): Resource[F, ClientId => ProducerMetrics[F]] =
    for {
      registry <- KafkaMetricsRegistry.of(prometheus, prefix)
    } yield { (clientId: ClientId) =>
      val source = sourceOf(clientId)

      new ProducerMetrics[F] {
        override def initTransactions(latency: FiniteDuration): F[Unit] = source.initTransactions(latency)

        override def beginTransaction: F[Unit] = source.beginTransaction

        override def sendOffsetsToTransaction(latency: FiniteDuration): F[Unit] =
          source.sendOffsetsToTransaction(latency)

        override def commitTransaction(latency: FiniteDuration): F[Unit] = source.commitTransaction(latency)

        override def abortTransaction(latency: FiniteDuration): F[Unit] = source.abortTransaction(latency)

        override def send(topic: Topic, latency: FiniteDuration, bytes: Int): F[Unit] =
          source.send(topic, latency, bytes)

        override def block(topic: Topic, latency: FiniteDuration): F[Unit] = source.block(topic, latency)

        override def failure(topic: Topic, latency: FiniteDuration): F[Unit] = source.failure(topic, latency)

        override def partitions(topic: Topic, latency: FiniteDuration): F[Unit] = source.partitions(topic, latency)

        override def flush(latency: FiniteDuration): F[Unit] = source.flush(latency)

        override def exposeJavaMetrics(producer: Producer[F]): Resource[F, Unit] =
          registry.register(producer.clientMetrics)

      }
    }

  /**
    * Construct [[ProducerMetrics]] that will expose Java Kafka client metrics.
    *
    * @param source original [[ProducerMetrics]]
    * @param prometheus instance of Prometheus registry
    * @param prefix metric name prefix
    * @return [[ProducerMetrics]] that exposes Java Kafka client metrics
    */
  def withJavaClientMetrics[F[_]: Sync: ToTry: UUIDGen](
    source: ProducerMetrics[F],
    prometheus: CollectorRegistry,
    prefix: Option[String],
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
      * @param prometheus instance of Prometheus registry
      * @param prefix metric name prefix
      * @return [[ProducerMetrics]] that exposes Java Kafka client metrics
      */
    def exposeJavaClientMetrics(
      prometheus: CollectorRegistry,
      prefix: Option[String],
    )(implicit F: Sync[F], toTry: ToTry[F]): Resource[F, ProducerMetrics[F]] =
      withJavaClientMetrics(source, prometheus, prefix)

  }

}
