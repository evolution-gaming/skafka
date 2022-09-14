package com.evolutiongaming.skafka.consumer

import cats.effect.Resource
import cats.implicits._
import cats.{Applicative, Monad, ~>}
import com.evolutiongaming.skafka.{ClientId, Topic, TopicPartition}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.{CollectorRegistry, LabelNames, Quantiles}

import scala.concurrent.duration.FiniteDuration

trait ConsumerMetrics[F[_]] {

  def call(name: String, topic: Topic, latency: FiniteDuration, success: Boolean): F[Unit]

  def poll(topic: Topic, bytes: Int, records: Int, age: Option[FiniteDuration]): F[Unit]

  def count(name: String, topic: Topic): F[Unit]

  def rebalance(name: String, topicPartition: TopicPartition): F[Unit]

  def topics(latency: FiniteDuration): F[Unit]
}

object ConsumerMetrics {

  type Prefix = String

  object Prefix {
    val Default: Prefix = "skafka_consumer"
  }

  def empty[F[_]: Applicative]: ConsumerMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): ConsumerMetrics[F] = new ConsumerMetrics[F] {

    def call(name: String, topic: Topic, latency: FiniteDuration, success: Boolean) = unit

    def poll(topic: Topic, bytes: Int, records: Int, age: Option[FiniteDuration]) = unit

    def count(name: String, topic: Topic) = unit

    def rebalance(name: String, topicPartition: TopicPartition) = unit

    def topics(latency: FiniteDuration) = unit
  }

  def of[F[_]: Monad](
    registry: CollectorRegistry[F],
    prefix: Prefix = Prefix.Default
  ): Resource[F, ClientId => ConsumerMetrics[F]] = {

    val callsCounter = registry.counter(
      name   = s"${prefix}_calls",
      help   = "Number of topic calls",
      labels = LabelNames("client", "topic", "type")
    )

    val resultCounter = registry.counter(
      name   = s"${prefix}_results",
      help   = "Topic call result: success or failure",
      labels = LabelNames("client", "topic", "type", "result")
    )

    val latencySummary = registry.summary(
      name      = s"${prefix}_latency",
      help      = "Topic call latency in seconds",
      quantiles = Quantiles.Default,
      labels    = LabelNames("client", "topic", "type")
    )

    val recordsSummary = registry.summary(
      name      = s"${prefix}_poll_records",
      help      = "Number of records per poll",
      quantiles = Quantiles.Empty,
      labels    = LabelNames("client", "topic")
    )

    val bytesSummary = registry.summary(
      name      = s"${prefix}_poll_bytes",
      help      = "Number of bytes per poll",
      quantiles = Quantiles.Empty,
      labels    = LabelNames("client", "topic")
    )

    val rebalancesCounter = registry.counter(
      name   = s"${prefix}_rebalances",
      help   = "Number of rebalances",
      labels = LabelNames("client", "topic", "type")
    )

    val topicsLatency = registry.summary(
      name      = s"${prefix}_topics_latency",
      help      = "List topics latency in seconds",
      quantiles = Quantiles.Default,
      labels    = LabelNames("client")
    )

    for {
      callsCounter      <- callsCounter
      resultCounter     <- resultCounter
      latencySummary    <- latencySummary
      recordsSummary    <- recordsSummary
      bytesSummary      <- bytesSummary
      rebalancesCounter <- rebalancesCounter
      topicsLatency     <- topicsLatency
      ageSummary        <- registry.summary(
        name = s"${ prefix }_poll_age",
        help = "Poll records age, time since record.timestamp",
        quantiles = Quantiles.Empty,
        labels = LabelNames("client", "topic")
      )
    } yield { clientId: ClientId =>
      new ConsumerMetrics[F] {

        def call(name: String, topic: Topic, latency: FiniteDuration, success: Boolean) = {
          val result = if (success) "success" else "failure"
          for {
            _ <- latencySummary.labels(clientId, topic, name).observe(latency.toNanos.nanosToSeconds)
            _ <- resultCounter.labels(clientId, topic, name, result).inc()
          } yield {}
        }

        def poll(topic: Topic, bytes: Int, records: Int, age: Option[FiniteDuration]) = {
          for {
            _ <- recordsSummary
              .labels(clientId, topic)
              .observe(records.toDouble)
            _ <- bytesSummary
              .labels(clientId, topic)
              .observe(bytes.toDouble)
            a <- age.foldMapM { age =>
              ageSummary
                .labels(clientId, topic)
                .observe(age.toNanos.nanosToSeconds)
            }
          } yield a
        }

        def count(name: String, topic: Topic) = {
          callsCounter
            .labels(clientId, topic, name)
            .inc()
        }

        def rebalance(name: String, topicPartition: TopicPartition) = {
          rebalancesCounter
            .labels(clientId, topicPartition.topic, name)
            .inc()
        }

        def topics(latency: FiniteDuration) = {
          topicsLatency
            .labels(clientId)
            .observe(latency.toNanos.nanosToSeconds)
        }
      }
    }
  }

  private sealed abstract class MapK

  implicit class ConsumerMetricsOps[F[_]](val self: ConsumerMetrics[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ConsumerMetrics[G] = {
      new MapK with ConsumerMetrics[G] {

        def call(name: String, topic: Topic, latency: FiniteDuration, success: Boolean) = {
          f(self.call(name, topic, latency, success))
        }

        def poll(topic: Topic, bytes: Int, records: Int, age: Option[FiniteDuration]) = {
          f(self.poll(topic, bytes, records, age))
        }

        def count(name: String, topic: Topic) = {
          f(self.count(name, topic))
        }

        def rebalance(name: String, topicPartition: TopicPartition) = {
          f(self.rebalance(name, topicPartition))
        }

        def topics(latency: FiniteDuration) = {
          f(self.topics(latency))
        }
      }
    }
  }
}
