package com.evolutiongaming.skafka.producer

import cats.effect.{Resource, Sync}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.metrics.KafkaMetricsCollector
import io.prometheus.client.CollectorRegistry

import scala.concurrent.duration.FiniteDuration

object ProducerMetricsOf {

  def withJavaClientMetrics[F[_]: Sync: ToTry](
    source: ProducerMetrics[F],
    prefix: String,
    prometheus: CollectorRegistry
  ): ProducerMetrics[F] =
    new ProducerMetrics[F] {
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

      override def exposeJavaMetrics(producer: Producer[F]): Resource[F, Unit] = {
        val collector = new KafkaMetricsCollector[F](producer.clientMetrics, prefix.some)
        Resource.make {
          Sync[F].delay { prometheus.register(collector) }
        } { _ =>
          Sync[F].delay { prometheus.unregister(collector) }
        }
      }

    }

  implicit final class Syntax[F[_]](val source: ProducerMetrics[F]) extends AnyVal {

    def exposeJavaClientMetrics(
      prefix: String,
      prometheus: CollectorRegistry
    )(implicit F: Sync[F], toTry: ToTry[F]): ProducerMetrics[F] = withJavaClientMetrics(source, prefix, prometheus)

  }

}
