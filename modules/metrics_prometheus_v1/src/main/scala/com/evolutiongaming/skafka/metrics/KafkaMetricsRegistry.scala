package com.evolution.skafka.metrics

import cats.syntax.all._
import cats.effect.syntax.resource._
import cats.effect.{Resource, Ref, Sync}
import cats.effect.std.UUIDGen
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.ClientMetric
import io.prometheus.metrics.model.registry.PrometheusRegistry

import java.util.UUID

/** Allows reporting metrics of multiple Kafka clients inside a single VM.
  *
  * Example:
  * {{{
  *   val prometheus: CollectorRegistry = ???
  *   val consumerOf: ConsumerOf[F] = ???
  *   val producerOf: ProducerOf[F] = ???
  *
  *   for {
  *     registry <- KafkaMetricsRegistry.of(prometheus)
  *
  *     consumer <- consumerOf(consumerConfig)
  *     _        <- registry.register(consumer.clientMetrics)
  *
  *     producer <- producerOf(producerConfig)
  *     _        <- registry.register(producer.clientMetrics)
  *   } yield ()
  * }}}
  */
trait KafkaMetricsRegistry[F[_]] {

  /** Register a function to obtain a list of client metrics. Normally, you would pass
    * [[com.evolutiongaming.skafka.consumer.Consumer#clientMetrics]] or
    * [[com.evolutiongaming.skafka.producer.Producer#clientMetrics]]
    */
  def register(metrics: F[Seq[ClientMetric[F]]]): Resource[F, Unit]
}

object KafkaMetricsRegistry {

  def of[F[_]: Sync: ToTry: UUIDGen](
    prometheus: PrometheusRegistry,
    prefix: Option[String] = None,
  ): Resource[F, KafkaMetricsRegistry[F]] =
    for {
      sources <- Ref[F].of(Map.empty[UUID, F[Seq[ClientMetric[F]]]]).toResource

      metrics = sources
        .get
        .flatMap { sources =>
          sources
            .toList
            .flatTraverse {
              case (uuid, metrics) =>
                metrics.map { metrics =>
                  metrics.toList.map { metric => metric.copy(tags = metric.tags + ("uuid" -> uuid.toString)) }
                }
            }
        }
        .widen[Seq[ClientMetric[F]]]

      collector = new KafkaMetricsCollector[F](metrics, prefix, List.empty)
      allocate  = Sync[F].delay { prometheus.register(collector) }
      release   = Sync[F].delay { prometheus.unregister(collector) }

      _ <- Resource.make(allocate)(_ => release)
    } yield new KafkaMetricsRegistry[F] {

      def register(metrics: F[Seq[ClientMetric[F]]]): Resource[F, Unit] =
        for {
          uuid <- UUIDGen[F].randomUUID.toResource

          allocate = sources.update { sources => sources.updated(uuid, metrics) }
          release  = sources.update { sources => sources - uuid }

          _ <- Resource.make(allocate)(_ => release)
        } yield {}
    }

}
