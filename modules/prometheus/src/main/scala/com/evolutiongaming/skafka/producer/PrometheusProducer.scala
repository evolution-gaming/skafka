package com.evolutiongaming.skafka.producer

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.skafka.PrometheusHelper._
import com.evolutiongaming.skafka.ToBytes
import io.prometheus.client.{CollectorRegistry, Counter, Summary}

import scala.compat.Platform
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

@deprecated("use PrometheusProducerMetrics", "2.1.0")
object PrometheusProducer {

  type Prefix = String

  object Prefix {
    val Default: Prefix = "skafka_producer"
  }


  private var metricsByPrefix: Map[(Prefix, CollectorRegistry), Metrics] = Map.empty


  def apply(
    producer: Producer,
    registry: CollectorRegistry,
    prefix: Prefix = Prefix.Default,
    clientId: String = ""): Producer = {

    val key = (prefix, registry)

    def metricsOrElse(create: => Metrics) = metricsByPrefix.getOrElse(key, create)

    val metrics = metricsOrElse {
      val register = Metrics.of(prefix)
      synchronized {
        metricsOrElse {
          val metrics = register(registry)
          metricsByPrefix = metricsByPrefix.updated(key, metrics)
          metrics
        }
      }
    }

    apply(producer, metrics, clientId)
  }

  def apply(
    producer: Producer,
    metrics: Metrics,
    clientId: String): Producer = {

    implicit val ec = CurrentThreadExecutionContext

    def latency[T](name: String)(f: => Future[T]) = {
      metrics
        .callLatency
        .labels(clientId, name)
        .time(f)
    }

    new Producer {

      def send[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

        val start = Platform.currentTime
        val result = producer.send(record)(valueToBytes, keyToBytes)
        result.onComplete { result =>
          val topic = record.topic
          val duration = (Platform.currentTime - start).toSeconds
          metrics.latency
            .labels(clientId, topic)
            .observe(duration)

          val resultLabel = result match {
            case Success(metadata) =>
              metrics.bytes
                .labels(clientId, topic)
                .observe(metadata.valueSerializedSize.getOrElse(0).toDouble)
              "success"
            case Failure(_)        =>
              "failure"
          }
          metrics.results
            .labels(clientId, topic, resultLabel)
            .inc()
        }
        result
      }

      def flush(): Future[Unit] = {
        latency("flush") { producer.flush() }
      }


      def close(timeout: FiniteDuration): Future[Unit] = {
        latency("close") { producer.close(timeout) }
      }

      def close(): Future[Unit] = {
        latency("close") { producer.close() }
      }
    }
  }


  final case class Metrics(
    latency: Summary,
    bytes: Summary,
    results: Counter,
    callLatency: Summary)

  object Metrics {

    def of(prefix: Prefix = Prefix.Default): CollectorRegistry => Metrics = {
      val latency = Summary.build()
        .name(s"${ prefix }_latency")
        .help("Latency in seconds")
        .labelNames("client", "topic")
        .quantile(0.5, 0.05)
        .quantile(0.9, 0.01)
        .quantile(0.99, 0.001)

      val bytes = Summary.build()
        .name(s"${ prefix }_bytes")
        .help("Message size in bytes")
        .labelNames("client", "topic")

      val results = Counter.build()
        .name(s"${ prefix }_result")
        .help("Result: success or failure")
        .labelNames("client", "topic", "result")

      val callLatency = Summary.build()
        .name(s"${ prefix }_call_latency")
        .help("Call latency in seconds")
        .labelNames("client", "type")
        .quantile(0.5, 0.05)
        .quantile(0.9, 0.01)
        .quantile(0.99, 0.001)

      registry: CollectorRegistry => {
        Metrics(
          latency = latency.register(registry),
          bytes = bytes.register(registry),
          results = results.register(registry),
          callLatency = callLatency.register(registry))
      }
    }
  }
}
