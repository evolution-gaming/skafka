package com.evolutiongaming.skafka.consumer

import cats.data.NonEmptyList
import cats.effect.{MonadCancel, Resource}
import cats.implicits.*
import cats.{Applicative, Monad, ~>}
import com.evolutiongaming.skafka.producer.ProducerMetrics
import com.evolutiongaming.skafka.{ClientId, Topic, TopicPartition}
import com.evolutiongaming.smetrics.MetricsHelper.*
import com.evolutiongaming.smetrics.*

import scala.concurrent.duration.FiniteDuration
import scala.annotation.nowarn

trait ConsumerMetrics[F[_]] {

  def call(name: String, topic: Topic, latency: FiniteDuration, success: Boolean): F[Unit]

  def poll(topic: Topic, bytes: Int, records: Int, age: Option[FiniteDuration]): F[Unit]

  def count(name: String, topic: Topic): F[Unit]

  def rebalance(name: String, topicPartition: TopicPartition): F[Unit]

  def topics(latency: FiniteDuration): F[Unit]

  private[consumer] def exposeJavaMetrics[K, V](@nowarn consumer: Consumer[F, K, V]): Resource[F, Unit] =
    Resource.unit[F]
}

object ConsumerMetrics {

  type Prefix = String

  object Prefix {
    val Default: Prefix = "skafka_consumer"
  }

  def empty[F[_]: Applicative]: ConsumerMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): ConsumerMetrics[F] = new Const(unit)

  private final class Const[F[_]](unit: F[Unit]) extends ConsumerMetrics[F] {
    override def call(name: String, topic: Topic, latency: FiniteDuration, success: Boolean): F[Unit] = unit

    override def poll(topic: Topic, bytes: Int, records: Int, age: Option[FiniteDuration]): F[Unit] = unit

    override def count(name: String, topic: Topic): F[Unit] = unit

    override def rebalance(name: String, topicPartition: TopicPartition): F[Unit] = unit

    override def topics(latency: FiniteDuration): F[Unit] = unit
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
      ageSummary <- registry.summary(
        name      = s"${prefix}_poll_age",
        help      = "Poll records age, time since record.timestamp",
        quantiles = Quantiles.Default,
        labels    = LabelNames("client", "topic")
      )
    } yield { (clientId: ClientId) =>
      new Summaries(
        callsCounter,
        resultCounter,
        latencySummary,
        recordsSummary,
        bytesSummary,
        rebalancesCounter,
        topicsLatency,
        ageSummary,
        clientId
      )
    }
  }

  private final class Summaries[F[_]: Monad](
    callsCounter: LabelValues.`3`[Counter[F]],
    resultCounter: LabelValues.`4`[Counter[F]],
    latencySummary: LabelValues.`3`[Summary[F]],
    recordsSummary: LabelValues.`2`[Summary[F]],
    bytesSummary: LabelValues.`2`[Summary[F]],
    rebalancesCounter: LabelValues.`3`[Counter[F]],
    topicsLatency: LabelValues.`1`[Summary[F]],
    ageSummary: LabelValues.`2`[Summary[F]],
    clientId: ClientId,
  ) extends ConsumerMetrics[F] {
    override def call(name: String, topic: Topic, latency: FiniteDuration, success: Boolean): F[Unit] = {
      val result = if (success) "success" else "failure"
      for {
        _ <- latencySummary.labels(clientId, topic, name).observe(latency.toNanos.nanosToSeconds)
        _ <- resultCounter.labels(clientId, topic, name, result).inc()
      } yield {}
    }

    override def poll(topic: Topic, bytes: Int, records: Int, age: Option[FiniteDuration]): F[Unit] = {
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

    override def count(name: String, topic: Topic): F[Unit] = {
      callsCounter
        .labels(clientId, topic, name)
        .inc()
    }

    override def rebalance(name: String, topicPartition: TopicPartition): F[Unit] = {
      rebalancesCounter
        .labels(clientId, topicPartition.topic, name)
        .inc()
    }

    override def topics(latency: FiniteDuration): F[Unit] = {
      topicsLatency
        .labels(clientId)
        .observe(latency.toNanos.nanosToSeconds)
    }
  }

  def histograms[F[_]: Monad](
    registry: CollectorRegistry[F],
    prefix: Prefix = Prefix.Default
  ): Resource[F, ClientId => ConsumerMetrics[F]] = {

    val callsCounter = registry.counter(
      name   = s"${prefix}_calls_total",
      help   = "Number of topic calls",
      labels = LabelNames("client", "topic", "type")
    )

    val resultCounter = registry.counter(
      name   = s"${prefix}_results_total",
      help   = "Topic call result: success or failure",
      labels = LabelNames("client", "topic", "type", "result")
    )

    val latencyHistogram = registry.histogram(
      name    = s"${prefix}_latency_seconds",
      help    = "Topic call latency in seconds",
      buckets = latencyBuckets,
      labels  = LabelNames("client", "topic", "type")
    )

    val recordsHistogram = registry.histogram(
      name    = s"${prefix}_poll_records_total",
      help    = "Number of records per poll",
      buckets = pollCountBuckets,
      labels  = LabelNames("client", "topic")
    )

    val bytesHistogram = registry.histogram(
      name    = s"${prefix}_poll_bytes",
      help    = "Number of bytes per poll",
      buckets = pollBytesBuckets,
      labels  = LabelNames("client", "topic")
    )

    val rebalancesCounter = registry.counter(
      name   = s"${prefix}_rebalances_total",
      help   = "Number of rebalances",
      labels = LabelNames("client", "topic", "type")
    )

    val topicsLatency = registry.histogram(
      name    = s"${prefix}_topics_latency_seconds",
      help    = "List topics latency in seconds",
      buckets = latencyBuckets,
      labels  = LabelNames("client")
    )
    val ageHistogram = registry.histogram(
      name    = s"${prefix}_poll_age_seconds",
      help    = "Poll records age, time since record.timestamp",
      buckets = pollAgeBuckets,
      labels  = LabelNames("client", "topic")
    )

    for {
      callsCounter      <- callsCounter
      resultCounter     <- resultCounter
      latencyHistogram  <- latencyHistogram
      recordsHistogram  <- recordsHistogram
      bytesHistogram    <- bytesHistogram
      rebalancesCounter <- rebalancesCounter
      topicsLatency     <- topicsLatency
      ageHistogram      <- ageHistogram
    } yield { (clientId: ClientId) =>
      new Histograms(
        callsCounter,
        resultCounter,
        latencyHistogram,
        recordsHistogram,
        bytesHistogram,
        rebalancesCounter,
        topicsLatency,
        ageHistogram,
        clientId
      )
    }
  }
  private val latencyBuckets =
    ProducerMetrics.latencyBuckets
  private val pollCountBuckets =
    Buckets(NonEmptyList.of(1, 20, 50, 200, 500))
  private val pollBytesBuckets =
    Buckets(
      NonEmptyList.of(
        2 * 1024,
        32 * 1024,
        64 * 1024,
        128 * 1024,
        512 * 1024,
        1024 * 1024, // 1 MB – max record size
        2 * 1024 * 1024,
        32 * 1024 * 1024,
        512 * 1024 * 1024, // 500 records per poll – the max poll size defaults
      )
    )
  private val pollAgeBuckets =
    Buckets(NonEmptyList.of(10e-3, 20e-3, 50e-3, 200e-3, 500e-3, 2, 5, 20, 60, 120))

  private final class Histograms[F[_]: Monad](
    callsCounter: LabelValues.`3`[Counter[F]],
    resultCounter: LabelValues.`4`[Counter[F]],
    latencySummary: LabelValues.`3`[Histogram[F]],
    recordsSummary: LabelValues.`2`[Histogram[F]],
    bytesSummary: LabelValues.`2`[Histogram[F]],
    rebalancesCounter: LabelValues.`3`[Counter[F]],
    topicsLatency: LabelValues.`1`[Histogram[F]],
    ageSummary: LabelValues.`2`[Histogram[F]],
    clientId: ClientId,
  ) extends ConsumerMetrics[F] {
    override def call(name: String, topic: Topic, latency: FiniteDuration, success: Boolean): F[Unit] = {
      val result = if (success) "success" else "failure"
      for {
        _ <- latencySummary.labels(clientId, topic, name).observe(latency.toNanos.nanosToSeconds)
        _ <- resultCounter.labels(clientId, topic, name, result).inc()
      } yield {}
    }

    override def poll(topic: Topic, bytes: Int, records: Int, age: Option[FiniteDuration]): F[Unit] = {
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

    override def count(name: String, topic: Topic): F[Unit] = {
      callsCounter
        .labels(clientId, topic, name)
        .inc()
    }

    override def rebalance(name: String, topicPartition: TopicPartition): F[Unit] = {
      rebalancesCounter
        .labels(clientId, topicPartition.topic, name)
        .inc()
    }

    override def topics(latency: FiniteDuration): F[Unit] = {
      topicsLatency
        .labels(clientId)
        .observe(latency.toNanos.nanosToSeconds)
    }
  }

  private sealed abstract class MapK

  implicit class ConsumerMetricsOps[F[_]](val self: ConsumerMetrics[F]) extends AnyVal {

    @deprecated("Use mapK(f, g) instead", "16.2.0")
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

    def mapK[G[_]](
      fg: F ~> G,
      gf: G ~> F
    )(implicit F: MonadCancel[F, Throwable], G: MonadCancel[G, Throwable]): ConsumerMetrics[G] =
      new MappedK(self, fg, gf)
  }

  private final class MappedK[F[_]: MonadCancel[*[_], Throwable], G[_]: MonadCancel[*[_], Throwable]](
    delegate: ConsumerMetrics[F],
    fg: F ~> G,
    gf: G ~> F
  ) extends ConsumerMetrics[G] {
    override def call(name: String, topic: Topic, latency: FiniteDuration, success: Boolean): G[Unit] = {
      fg(delegate.call(name, topic, latency, success))
    }

    override def poll(topic: Topic, bytes: Int, records: Int, age: Option[FiniteDuration]): G[Unit] = {
      fg(delegate.poll(topic, bytes, records, age))
    }

    override def count(name: String, topic: Topic): G[Unit] = {
      fg(delegate.count(name, topic))
    }

    override def rebalance(name: String, topicPartition: TopicPartition): G[Unit] = {
      fg(delegate.rebalance(name, topicPartition))
    }

    override def topics(latency: FiniteDuration): G[Unit] = {
      fg(delegate.topics(latency))
    }

    override def exposeJavaMetrics[K, V](consumer: Consumer[G, K, V]): Resource[G, Unit] =
      delegate.exposeJavaMetrics(consumer.mapK(gf, fg)).mapK(fg)
  }
}
