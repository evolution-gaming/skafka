package com.evolutiongaming.skafka.producer

import cats.effect.Resource
import cats.implicits._
import cats.{Applicative, Monad, ~>}
import com.evolutiongaming.skafka.{ClientId, Topic}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.{CollectorRegistry, LabelNames, Quantile, Quantiles}

import scala.concurrent.duration.FiniteDuration

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
}

object ProducerMetrics {

  type Prefix = String

  object Prefix {
    val Default: Prefix = "skafka_producer"
  }

  def empty[F[_]: Applicative]: ProducerMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): ProducerMetrics[F] = new ProducerMetrics[F] {

    def initTransactions(latency: FiniteDuration) = unit

    val beginTransaction = unit

    def sendOffsetsToTransaction(latency: FiniteDuration) = unit

    def commitTransaction(latency: FiniteDuration) = unit

    def abortTransaction(latency: FiniteDuration) = unit

    def block(topic: Topic, latency: FiniteDuration) = unit

    def send(topic: Topic, latency: FiniteDuration, bytes: Int) = unit

    def failure(topic: Topic, latency: FiniteDuration) = unit

    def partitions(topic: Topic, latency: FiniteDuration) = unit

    def flush(latency: FiniteDuration) = unit
  }

  def of[F[_]: Monad](
    registry: CollectorRegistry[F],
    prefix: Prefix = Prefix.Default
  ): Resource[F, ClientId => ProducerMetrics[F]] = {

    val latencySummary = registry.summary(
      name      = s"${prefix}_latency",
      help      = "Latency in seconds",
      quantiles = Quantiles(Quantile(value = 0.9, error = 0.05), Quantile(value = 0.99, error = 0.005)),
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
      quantiles = Quantiles(Quantile(value = 0.9, error = 0.05), Quantile(value = 0.99, error = 0.005)),
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
    } yield { clientId: ClientId =>
      {

        def sendMeasure(result: String, topic: Topic, latency: FiniteDuration) = {
          for {
            _ <- latencySummary.labels(clientId, topic, "send").observe(latency.toNanos.nanosToSeconds)
            _ <- resultCounter.labels(clientId, topic, result).inc()
          } yield {}
        }

        def observeLatency(name: String, latency: FiniteDuration) = {
          callLatency
            .labels(clientId, name)
            .observe(latency.toNanos.nanosToSeconds)
        }

        new ProducerMetrics[F] {

          def initTransactions(latency: FiniteDuration) = {
            observeLatency("init_transactions", latency)
          }

          val beginTransaction = {
            callCount.labels(clientId, "begin_transaction").inc()
          }

          def sendOffsetsToTransaction(latency: FiniteDuration) = {
            observeLatency("send_offsets", latency)
          }

          def commitTransaction(latency: FiniteDuration) = {
            observeLatency("commit_transaction", latency)
          }

          def abortTransaction(latency: FiniteDuration) = {
            observeLatency("abort_transaction", latency)
          }

          def block(topic: Topic, latency: FiniteDuration) = {
            latencySummary
              .labels(clientId, topic, "block")
              .observe(latency.toNanos.nanosToSeconds)
          }

          def send(topic: Topic, latency: FiniteDuration, bytes: Int) = {
            for {
              _ <- sendMeasure(result = "success", topic = topic, latency = latency)
              _ <- bytesSummary.labels(clientId, topic).observe(bytes.toDouble)
            } yield {}
          }

          def failure(topic: Topic, latency: FiniteDuration) = {
            sendMeasure(result = "failure", topic = topic, latency = latency)
          }

          def partitions(topic: Topic, latency: FiniteDuration) = {
            latencySummary
              .labels(clientId, topic, "partitions")
              .observe(latency.toNanos.nanosToSeconds)
          }

          def flush(latency: FiniteDuration) = {
            observeLatency("flush", latency)
          }
        }
      }
    }
  }

  implicit class ProducerMetricsOps[F[_]](val self: ProducerMetrics[F]) extends AnyVal {

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
  }
}
