package com.evolutiongaming.skafka.producer

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.skafka.ToBytes
import io.prometheus.client.{CollectorRegistry, Counter, Summary}

import scala.compat.Platform
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object PrometheusProducer {

  private var collectors: Collectors = _

  private def registerCollectors(registry: CollectorRegistry): Unit = synchronized {
    if (collectors == null)
      collectors = Builders().register(registry)
  }

  def apply(
    producer: Producer,
    registry: CollectorRegistry): Producer = {

    registerCollectors(registry)

    implicit val ec: ExecutionContext = CurrentThreadExecutionContext

    new Producer {

      def send[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

        val start = Platform.currentTime
        val result = producer.send(record)(valueToBytes, keyToBytes)
        result.onComplete { result =>
          val topicLabel = record.topic.replace(".", "_")
          val duration = (Platform.currentTime - start).toDouble / 1000
          collectors.latencySummary
            .labels(topicLabel)
            .observe(duration)

          val resultLabel = result match {
            case Success(metadata) =>
              collectors.bytesSummary
                .labels(topicLabel)
                .observe(metadata.valueSerializedSize.getOrElse(0).toDouble)
              "success"
            case Failure(_)        =>
              "failure"
          }
          collectors.counter
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

  private case class Builders(
    latencySummary: Summary.Builder = Summary.build()
      .name("skafka_producer_latency")
      .help("Latency in seconds")
      .labelNames("topic")
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001),

    bytesSummary: Summary.Builder = Summary.build()
      .name("skafka_producer_bytes")
      .help("Message size in bytes")
      .labelNames("topic"),

    counter: Counter.Builder = Counter.build()
      .name("skafka_producer_result")
      .help("Result: success or failure")
      .labelNames("topic", "result")) {

    def register(registry: CollectorRegistry): Collectors = {
      Collectors(
        latencySummary = latencySummary.register(registry),
        bytesSummary = bytesSummary.register(registry),
        counter = counter.register(registry))
    }
  }

  private case class Collectors(
    latencySummary: Summary,
    bytesSummary: Summary,
    counter: Counter)
}
