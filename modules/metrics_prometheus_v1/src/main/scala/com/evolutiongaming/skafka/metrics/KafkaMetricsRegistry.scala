package com.evolution.skafka.metrics

import cats.syntax.all._
import cats.effect.syntax.resource._
import cats.effect.{Resource, Ref, Sync}
import cats.effect.std.UUIDGen
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.ClientMetric
import io.prometheus.metrics.model.registry.PrometheusRegistry

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

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

  private val activeRegistrations = new ConcurrentHashMap[(PrometheusRegistry, Option[String]), Unit]()

  def of[F[_]: Sync: ToTry: UUIDGen](
    prometheus: PrometheusRegistry,
    prefix: Option[String] = None,
  ): Resource[F, KafkaMetricsRegistry[F]] = {
    val key = (prometheus, prefix)

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

      _ <- Resource.make {
        Sync[F].delay {
          val previous = Option(activeRegistrations.putIfAbsent(key, ()))
          if (previous.isDefined) {
            throw new IllegalStateException(
              s"KafkaMetricsRegistry already registered for this PrometheusRegistry instance with prefix=$prefix"
            )
          }
          prometheus.register(collector)
        }
      } { _ =>
        Sync[F].delay {
          prometheus.unregister(collector)
          activeRegistrations.remove(key)
        }.void
      }
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

}
