package com.evolutiongaming.skafka.producer

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.skafka.ToBytes
import io.prometheus.client.{CollectorRegistry, Counter, Summary}

import scala.compat.Platform
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object PrometheusProducer {

  type Prefix = String

  private var metricsByPrefix: Map[Prefix, Metrics] = Map.empty

  def apply(
    producer: Producer,
    registry: CollectorRegistry,
    prefix: Prefix = "skafka_producer",
    label: String = ""): Producer = {

    def metricsOrElse(create: => Metrics) = metricsByPrefix.getOrElse(prefix, create)

    val metrics = metricsOrElse {
      val register = build(prefix)
      synchronized {
        metricsOrElse {
          val metrics = register(registry)
          metricsByPrefix = metricsByPrefix.updated(prefix, metrics)
          metrics
        }
      }
    }

    apply(producer, metrics, label)
  }

  def apply(
    producer: Producer,
    metrics: Metrics,
    label: String): Producer = {

    implicit val ec = CurrentThreadExecutionContext

    new Producer {

      def send[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

        val start = Platform.currentTime
        val result = producer.send(record)(valueToBytes, keyToBytes)
        result.onComplete { result =>
          val topicLabel = record.topic.replace(".", "_")
          val duration = (Platform.currentTime - start).toDouble / 1000
          metrics.latencySummary
            .labels(label, topicLabel)
            .observe(duration)

          val resultLabel = result match {
            case Success(metadata) =>
              metrics.bytesSummary
                .labels(label, topicLabel)
                .observe(metadata.valueSerializedSize.getOrElse(0).toDouble)
              "success"
            case Failure(_)        =>
              "failure"
          }
          metrics.counter
            .labels(label, topicLabel, resultLabel)
            .inc()
        }
        result
      }

      def flush(): Future[Unit] = {
        producer.flush()
      }

      def close(timeout: FiniteDuration): Future[Unit] = {
        producer.close(timeout)
      }

      def close(): Future[Unit] = {
        producer.close()
      }
    }
  }

  private def build(prefix: Prefix) = {
    val latencySummary = Summary.build()
      .name(s"${ prefix }_latency")
      .help("Latency in seconds")
      .labelNames("producer", "topic")
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)

    val bytesSummary = Summary.build()
      .name(s"${ prefix }_bytes")
      .help("Message size in bytes")
      .labelNames("producer", "topic")

    val counter = Counter.build()
      .name(s"${ prefix }_result")
      .help("Result: success or failure")
      .labelNames("producer", "topic", "result")

    registry: CollectorRegistry => {
      Metrics(
        latencySummary = latencySummary.register(registry),
        bytesSummary = bytesSummary.register(registry),
        counter = counter.register(registry))
    }
  }

  final case class Metrics(
    latencySummary: Summary,
    bytesSummary: Summary,
    counter: Counter)
}
