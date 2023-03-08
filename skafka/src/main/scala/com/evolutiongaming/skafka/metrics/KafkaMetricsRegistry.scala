package com.evolutiongaming.skafka.metrics

import cats.effect.kernel.Sync
import cats.effect.{Ref, Resource}
import cats.syntax.all._
import com.evolutiongaming.skafka.ClientMetric

import java.util.UUID

/** Allows reporting metrics of multiple Kafka clients inside a single VM.
  *
  * Example:
  * {{{
  *   val consumerOf: ConsumerOf[F] = ???
  *   val producerOf: ProducerOf[F] = ???
  *   val kafkaRegistry: KafkaMetricsRegistry[F] = ???
  *
  *   for {
  *     consumer <- consumerOf.apply(consumerConfig)
  *     _        <- kafkaRegistry.register(consumer.clientMetrics)
  *
  *     producer <- producerOf.apply(producerConfig)
  *     _        <- kafkaRegistry.register(producer.clientMetrics)
  *
  *     metrics  <- kafkaRegistry.collectAll
  *   } yield ()
  * }}}
  *
  * To avoid manually registering each client there are syntax extension, wrapping `ProducerOf` and `ConsumerOf`,
  * see `com.evolutiongaming.skafka.metrics.syntax`.
  *
  * Example:
  * {{{
  *   val kafkaRegistry: KafkaMetricsRegistry[F] = ...
  *   val consumerOf: ConsumerOf[F] = ConsumerOf.apply1[F](...).withNativeMetrics(kafkaRegistry)
  *   val producerOf: ProducerOf[F] = ProducerOf.apply1[F](...).withNativeMetrics(kafkaRegistry)
  *
  *   for {
  *     consumer <- consumerOf.apply(consumerConfig).
  *     producer <- producerOf.apply(producerConfig)
  *     metrics  <- kafkaRegistry.collectAll
  *   } yield ()
  * }}}
  */
trait KafkaMetricsRegistry[F[_]] {

  /**
    * Register a function to obtain a list of client metrics.
    * Normally, you would pass [[com.evolutiongaming.skafka.consumer.Consumer.clientMetrics]] or
    * [[com.evolutiongaming.skafka.producer.Producer.clientMetrics]]
    *
    * @return synthetic ID of registered function
    */
  def register(metrics: F[Seq[ClientMetric[F]]]): Resource[F, UUID]

  /** Collect metrics from all registered functions */
  def collectAll: F[Seq[ClientMetric[F]]]
}

object KafkaMetricsRegistry {
  private final class FromRef[F[_]: Sync](ref: Ref[F, Map[UUID, F[Seq[ClientMetric[F]]]]], allowDuplicates: Boolean)
      extends KafkaMetricsRegistry[F] {
    override def register(metrics: F[Seq[ClientMetric[F]]]): Resource[F, UUID] = {
      val acquire: F[UUID] = for {
        id <- Sync[F].delay(UUID.randomUUID())
        _  <- ref.update(m => m + (id -> metrics))
      } yield id

      def release(id: UUID): F[Unit] =
        ref.update(m => m - id)

      Resource.make(acquire)(id => release(id))
    }

    override def collectAll: F[Seq[ClientMetric[F]]] =
      ref.get.flatMap { map: Map[UUID, F[Seq[ClientMetric[F]]]] =>
        map.values.toList.sequence.map { metrics =>
          val results: List[ClientMetric[F]] = metrics.flatten

          if (allowDuplicates) {
            results
          } else {
            results
              .groupBy(metric => (metric.name, metric.group, metric.tags))
              .map { case (_, values) => values.head }
              .toSeq
          }
        }
      }
  }

  def ref[F[_]: Sync](allowDuplicates: Boolean): F[KafkaMetricsRegistry[F]] = {
    Ref.of[F, Map[UUID, F[Seq[ClientMetric[F]]]]](Map.empty).map(ref => new FromRef[F](ref, allowDuplicates))
  }

  def ref[F[_]: Sync]: F[KafkaMetricsRegistry[F]] = ref[F](allowDuplicates = false)
}
