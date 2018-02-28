package com.evolutiongaming.skafka.producer

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import io.prometheus.client.{CollectorRegistry, Counter, Summary}

import scala.compat.Platform
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object PrometheusProducer {

  def apply(producer: Producer, registry: CollectorRegistry): Producer = {

    implicit val ec = CurrentThreadExecutionContext

    val prefix = "skafka_producer"

    val latencySummary = Summary.build()
      .name(s"${ prefix }_latency")
      .help("Latency in millis")
      .labelNames("topic")
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .register(registry)

    val bytesSummary = Summary.build()
      .name(s"${ prefix }_bytes")
      .help("Message size in bytes")
      .labelNames("topic")
      .register(registry)

    val counter = Counter.build()
      .name(s"${ prefix }_result")
      .help("Result: success or failure")
      .labelNames("topic", "result")
      .register(registry)

    new Producer {

      def doApply[K, V](record: Producer.Record[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {

        val start = Platform.currentTime
        val result = producer.doApply(record)(valueToBytes, keyToBytes)
        result.onComplete { result =>
          val topic = record.topic
          val duration = Platform.currentTime - start
          latencySummary.labels(topic).observe(duration.toDouble)
          val name = result match {
            case Success(metadata) => bytesSummary.observe(metadata.serializedValueSize.toDouble); "success"
            case Failure(_)        => "failure"
          }
          counter.labels(topic, name).inc()
        }
        result
      }

      def flush(): Future[Unit] = {
        producer.flush()
      }

      def closeAsync(timeout: FiniteDuration): Future[Unit] = {
        producer.closeAsync(timeout)
      }

      def close(): Unit = {
        producer.close()
      }
    }
  }
}
