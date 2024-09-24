package com.evolutiongaming.skafka.producer

import cats.data.NonEmptyList
import cats.effect.{MonadCancel, MonadCancelThrow, Resource}
import cats.implicits.*
import cats.{Applicative, Monad, ~>}
import com.evolutiongaming.skafka.{ClientId, Topic}
import com.evolutiongaming.smetrics.MetricsHelper.*
import com.evolutiongaming.smetrics.*

import scala.concurrent.duration.FiniteDuration
import scala.annotation.nowarn

trait ProducerMetrics[F[_]] {

  def initTransactions(latency: FiniteDuration): F[Unit]

  def beginTransaction: F[Unit]

  def sendOffsetsToTransaction(latency: FiniteDuration): F[Unit]

  def commitTransaction(latency: FiniteDuration): F[Unit]

  def abortTransaction(latency: FiniteDuration): F[Unit]

  def send(topic: Topic, latency: FiniteDuration, bytes: Int): F[Unit]

  def block(topic: Topic, latency: FiniteDuration): F[Unit]

  def failure(topic: Topic, latency: FiniteDuration): F[Unit]

  def partitions(topic: Topic, latency: FiniteDuration): F[Unit]

  def flush(latency: FiniteDuration): F[Unit]

  private[producer] def exposeJavaMetrics(@nowarn producer: Producer[F]): Resource[F, Unit] = Resource.unit[F]
}

object ProducerMetrics {

  type Prefix = String

  object Prefix {
    val Default: Prefix = "skafka_producer"
  }

  def empty[F[_]: Applicative]: ProducerMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): ProducerMetrics[F] = new Const[F](unit)

  private final class Const[F[_]](unit: F[Unit]) extends ProducerMetrics[F] {
    override def initTransactions(latency: FiniteDuration): F[Unit] = unit

    override def beginTransaction: F[Unit] = unit

    override def sendOffsetsToTransaction(latency: FiniteDuration): F[Unit] = unit

    override def commitTransaction(latency: FiniteDuration): F[Unit] = unit

    override def abortTransaction(latency: FiniteDuration): F[Unit] = unit

    override def block(topic: Topic, latency: FiniteDuration): F[Unit] = unit

    override def send(topic: Topic, latency: FiniteDuration, bytes: Int): F[Unit] = unit

    override def failure(topic: Topic, latency: FiniteDuration): F[Unit] = unit

    override def partitions(topic: Topic, latency: FiniteDuration): F[Unit] = unit

    override def flush(latency: FiniteDuration): F[Unit] = unit
  }

  def of[F[_]: Monad](
    registry: CollectorRegistry[F],
    prefix: Prefix = Prefix.Default
  ): Resource[F, ClientId => ProducerMetrics[F]] = {

    val latencySummary = registry.summary(
      name      = s"${prefix}_latency",
      help      = "Latency in seconds",
      quantiles = Quantiles.Default,
      labels    = LabelNames("client", "topic", "type")
    )

    val bytesSummary = registry.summary(
      name      = s"${prefix}_bytes",
      help      = "Message size in bytes",
      quantiles = Quantiles(Quantile(1.0, 0.0001)),
      labels    = LabelNames("client", "topic")
    )

    val resultCounter = registry.counter(
      name   = s"${prefix}_results",
      help   = "Result: success or failure",
      labels = LabelNames("client", "topic", "result")
    )

    val callLatency = registry.summary(
      name      = s"${prefix}_call_latency",
      help      = "Call latency in seconds",
      quantiles = Quantiles.Default,
      labels    = LabelNames("client", "type")
    )

    val callCount =
      registry.counter(name = s"${prefix}_calls", help = "Call count", labels = LabelNames("client", "type"))

    for {
      latencySummary <- latencySummary
      bytesSummary   <- bytesSummary
      resultCounter  <- resultCounter
      callLatency    <- callLatency
      callCount      <- callCount
    } yield { (clientId: ClientId) =>
      new Summaries[F](latencySummary, bytesSummary, resultCounter, callLatency, callCount, clientId)
    }
  }

  def histograms[F[_]: Monad](
    registry: CollectorRegistry[F],
    prefix: Prefix = Prefix.Default
  ): Resource[F, ClientId => ProducerMetrics[F]] = {
    val latencyHistogram = registry.histogram(
      name    = s"${prefix}_latency",
      help    = "Latency in seconds",
      buckets = latencyBuckets,
      labels  = LabelNames("client", "topic", "type")
    )
    val bytesHistogram = registry.histogram(
      name    = s"${prefix}_bytes",
      help    = "Message size in bytes",
      buckets = recordBytesBuckets,
      labels  = LabelNames("client", "topic")
    )
    val resultCounter = registry.counter(
      name   = s"${prefix}_results",
      help   = "Result: success or failure",
      labels = LabelNames("client", "topic", "result")
    )
    val callLatency = registry.histogram(
      name    = s"${prefix}_call_latency",
      help    = "Call latency in seconds",
      buckets = latencyBuckets,
      labels  = LabelNames("client", "type")
    )
    val callCount =
      registry.counter(name = s"${prefix}_calls_total", help = "Call count", labels = LabelNames("client", "type"))
    for {
      latencyHistogram <- latencyHistogram
      bytesHistogram   <- bytesHistogram
      resultCounter    <- resultCounter
      callLatency      <- callLatency
      callCount        <- callCount
    } yield { (clientId: ClientId) =>
      new Histograms[F](latencyHistogram, bytesHistogram, resultCounter, callLatency, callCount, clientId)
    }
  }
  private[skafka] val latencyBuckets =
    Buckets(
      NonEmptyList.of(
        250e-6, // 250 microseconds – lowest observed latency in practice for blocks
        500e-6,
        // millisecond precision
        2e-3, 5e-3, 20e-3, 50e-3, // 50 milliseconds – this is the median value for sending records
        200e-3, 500e-3,
        // second precision – starting from this point latencies are not "normal"
        1, 2, 5, 30, 60, // 1 minute – many timeouts are already triggered by this time
      )
    )
  private val recordBytesBuckets =
    Buckets(
      NonEmptyList.of(
        2 * 1024,
        32 * 1024,
        64 * 1024,
        128 * 1024,
        512 * 1024,
        1024 * 1024, // 1 MB – max record size
      )
    )

  private final class Summaries[F[_]: Monad](
    latencySummary: LabelValues.`3`[Summary[F]],
    bytesSummary: LabelValues.`2`[Summary[F]],
    resultCounter: LabelValues.`3`[Counter[F]],
    callLatency: LabelValues.`2`[Summary[F]],
    callCount: LabelValues.`2`[Counter[F]],
    clientId: ClientId
  ) extends ProducerMetrics[F] {
    override def initTransactions(latency: FiniteDuration): F[Unit] = {
      observeLatency("init_transactions", latency)
    }

    override def beginTransaction: F[Unit] = {
      callCount.labels(clientId, "begin_transaction").inc()
    }

    override def sendOffsetsToTransaction(latency: FiniteDuration): F[Unit] = {
      observeLatency("send_offsets", latency)
    }

    override def commitTransaction(latency: FiniteDuration): F[Unit] = {
      observeLatency("commit_transaction", latency)
    }

    override def abortTransaction(latency: FiniteDuration): F[Unit] = {
      observeLatency("abort_transaction", latency)
    }

    override def block(topic: Topic, latency: FiniteDuration): F[Unit] = {
      latencySummary
        .labels(clientId, topic, "block")
        .observe(latency.toNanos.nanosToSeconds)
    }

    override def send(topic: Topic, latency: FiniteDuration, bytes: Int): F[Unit] = {
      for {
        _ <- sendMeasure(result = "success", topic = topic, latency = latency)
        _ <- bytesSummary.labels(clientId, topic).observe(bytes.toDouble)
      } yield {}
    }

    override def failure(topic: Topic, latency: FiniteDuration): F[Unit] = {
      sendMeasure(result = "failure", topic = topic, latency = latency)
    }

    override def partitions(topic: Topic, latency: FiniteDuration): F[Unit] = {
      latencySummary
        .labels(clientId, topic, "partitions")
        .observe(latency.toNanos.nanosToSeconds)
    }

    override def flush(latency: FiniteDuration): F[Unit] = {
      observeLatency("flush", latency)
    }

    private def sendMeasure(result: String, topic: Topic, latency: FiniteDuration): F[Unit] = {
      for {
        _ <- latencySummary.labels(clientId, topic, "send").observe(latency.toNanos.nanosToSeconds)
        _ <- resultCounter.labels(clientId, topic, result).inc()
      } yield {}
    }

    private def observeLatency(name: String, latency: FiniteDuration): F[Unit] = {
      callLatency
        .labels(clientId, name)
        .observe(latency.toNanos.nanosToSeconds)
    }
  }

  private final class Histograms[F[_]: Monad](
    latencyHistogram: LabelValues.`3`[Histogram[F]],
    bytesHistogram: LabelValues.`2`[Histogram[F]],
    resultCounter: LabelValues.`3`[Counter[F]],
    callLatency: LabelValues.`2`[Histogram[F]],
    callCount: LabelValues.`2`[Counter[F]],
    clientId: ClientId,
  ) extends ProducerMetrics[F] {
    override def initTransactions(latency: FiniteDuration): F[Unit] = {
      observeLatency("init_transactions", latency)
    }

    override def beginTransaction: F[Unit] = {
      callCount.labels(clientId, "begin_transaction").inc()
    }

    override def sendOffsetsToTransaction(latency: FiniteDuration): F[Unit] = {
      observeLatency("send_offsets", latency)
    }

    override def commitTransaction(latency: FiniteDuration): F[Unit] = {
      observeLatency("commit_transaction", latency)
    }

    override def abortTransaction(latency: FiniteDuration): F[Unit] = {
      observeLatency("abort_transaction", latency)
    }

    override def block(topic: Topic, latency: FiniteDuration): F[Unit] = {
      latencyHistogram
        .labels(clientId, topic, "block")
        .observe(latency.toNanos.nanosToSeconds)
    }

    override def send(topic: Topic, latency: FiniteDuration, bytes: Int): F[Unit] = {
      for {
        _ <- sendMeasure(result = "success", topic = topic, latency = latency)
        _ <- bytesHistogram.labels(clientId, topic).observe(bytes.toDouble)
      } yield {}
    }

    override def failure(topic: Topic, latency: FiniteDuration): F[Unit] = {
      sendMeasure(result = "failure", topic = topic, latency = latency)
    }

    override def partitions(topic: Topic, latency: FiniteDuration): F[Unit] = {
      latencyHistogram
        .labels(clientId, topic, "partitions")
        .observe(latency.toNanos.nanosToSeconds)
    }

    override def flush(latency: FiniteDuration): F[Unit] = {
      observeLatency("flush", latency)
    }

    private def sendMeasure(result: String, topic: Topic, latency: FiniteDuration): F[Unit] = {
      for {
        _ <- latencyHistogram.labels(clientId, topic, "send").observe(latency.toNanos.nanosToSeconds)
        _ <- resultCounter.labels(clientId, topic, result).inc()
      } yield {}
    }

    private def observeLatency(name: String, latency: FiniteDuration): F[Unit] = {
      callLatency
        .labels(clientId, name)
        .observe(latency.toNanos.nanosToSeconds)
    }
  }

  implicit class ProducerMetricsOps[F[_]](val self: ProducerMetrics[F]) extends AnyVal {

    @deprecated("Use mapK(f, g) instead", "16.2.0")
    def mapK[G[_]](f: F ~> G): ProducerMetrics[G] = new ProducerMetrics[G] {

      def initTransactions(latency: FiniteDuration) = f(self.initTransactions(latency))

      def beginTransaction = f(self.beginTransaction)

      def sendOffsetsToTransaction(latency: FiniteDuration) = f(self.sendOffsetsToTransaction(latency))

      def commitTransaction(latency: FiniteDuration) = f(self.commitTransaction(latency))

      def abortTransaction(latency: FiniteDuration) = f(self.abortTransaction(latency))

      def send(topic: Topic, latency: FiniteDuration, bytes: Int) = f(self.send(topic, latency, bytes))

      def block(topic: Topic, latency: FiniteDuration) = f(self.block(topic, latency))

      def failure(topic: Topic, latency: FiniteDuration) = f(self.failure(topic, latency))

      def partitions(topic: Topic, latency: FiniteDuration) = f(self.partitions(topic, latency))

      def flush(latency: FiniteDuration) = f(self.flush(latency))
    }

    def mapK[G[_]](
      fg: F ~> G,
      gf: G ~> F
    )(implicit F: MonadCancel[F, Throwable], G: MonadCancel[G, Throwable]): ProducerMetrics[G] =
      new MappedK(self, fg, gf)
  }

  private final class MappedK[F[_]: MonadCancelThrow, G[_]: MonadCancelThrow](
    delegate: ProducerMetrics[F],
    fg: F ~> G,
    gf: G ~> F,
  ) extends ProducerMetrics[G] {
    override def initTransactions(latency: FiniteDuration): G[Unit] = fg(delegate.initTransactions(latency))

    override def beginTransaction: G[Unit] = fg(delegate.beginTransaction)

    override def sendOffsetsToTransaction(latency: FiniteDuration): G[Unit] =
      fg(delegate.sendOffsetsToTransaction(latency))

    override def commitTransaction(latency: FiniteDuration): G[Unit] = fg(delegate.commitTransaction(latency))

    override def abortTransaction(latency: FiniteDuration): G[Unit] = fg(delegate.abortTransaction(latency))

    override def send(topic: Topic, latency: FiniteDuration, bytes: Int): G[Unit] =
      fg(delegate.send(topic, latency, bytes))

    override def block(topic: Topic, latency: FiniteDuration): G[Unit] = fg(delegate.block(topic, latency))

    override def failure(topic: Topic, latency: FiniteDuration): G[Unit] = fg(delegate.failure(topic, latency))

    override def partitions(topic: Topic, latency: FiniteDuration): G[Unit] = fg(delegate.partitions(topic, latency))

    override def flush(latency: FiniteDuration): G[Unit] = fg(delegate.flush(latency))

    override def exposeJavaMetrics(producer: Producer[G]): Resource[G, Unit] =
      delegate.exposeJavaMetrics(producer.mapK[F](gf, fg)).mapK(fg)
  }
}
