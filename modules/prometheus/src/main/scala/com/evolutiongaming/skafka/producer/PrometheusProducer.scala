package com.evolutiongaming.skafka.producer

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.skafka.ToBytes
import io.prometheus.client.{CollectorRegistry, Counter, Summary}

import scala.compat.Platform
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object PrometheusProducer {

  private var latencySummary: Summary = _
  private var bytesSummary: Summary = _
  private var counter: Counter = _
  private var isInitDone = false

  private def initMetrics(registry: CollectorRegistry): Unit = {
    if (!isInitDone) {
      isInitDone = true

      latencySummary = Summary.build()
        .name("skafka_producer_latency")
        .help("Latency in seconds")
        .labelNames("topic")
        .quantile(0.9, 0.01)
        .quantile(0.99, 0.001)
        .register(registry)

      bytesSummary = Summary.build()
        .name("skafka_producer_bytes")
        .help("Message size in bytes")
        .labelNames("topic")
        .register(registry)

      counter = Counter.build()
        .name("skafka_producer_result")
        .help("Result: success or failure")
        .labelNames("topic", "result")
        .register(registry)
    }
  }

  def apply(
    producer: Producer,
    registry: CollectorRegistry): Producer = {

    initMetrics(registry)

    implicit val ec = CurrentThreadExecutionContext

    new Producer {

      def send[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

        val start = Platform.currentTime
        val result = producer.send(record)(valueToBytes, keyToBytes)
        result.onComplete { result =>
          val topicLabel = record.topic.replace(".", "_")
          val duration = (Platform.currentTime - start).toDouble / 1000
          latencySummary
            .labels(topicLabel)
            .observe(duration)

          val resultLabel = result match {
            case Success(metadata) =>
              bytesSummary
                .labels(topicLabel)
                .observe(metadata.valueSerializedSize.getOrElse(0).toDouble)
              "success"
            case Failure(_)        =>
              "failure"
          }
          counter
            .labels(topicLabel, resultLabel)
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
}
