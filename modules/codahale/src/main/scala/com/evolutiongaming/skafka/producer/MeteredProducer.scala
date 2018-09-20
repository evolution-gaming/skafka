package com.evolutiongaming.skafka.producer

import com.codahale.metrics.MetricRegistry
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.skafka.ToBytes
import com.evolutiongaming.util.MetricHelper._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

@deprecated("use PrometheusProducerMetrics or Producer.Metrics instead", "2.1.0")
object MeteredProducer {

  def apply(producer: Producer, registry: MetricRegistry): Producer = {

    implicit val ec = CurrentThreadExecutionContext

    new Producer {

      def send[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

        val topic = record.topic
        val result = registry.histogram(s"$topic.latency").timeFuture {
          producer.send(record)
        }

        result.onComplete {
          case Success(metadata) =>
            registry.meter(s"$topic.bytes").mark(metadata.valueSerializedSize.getOrElse(0).toLong)
            registry.counter(s"$topic.success").inc()

          case Failure(_) =>
            registry.counter(s"$topic.failure").inc()
        }

        result
      }

      def flush(): Future[Unit] = producer.flush()

      def close(timeout: FiniteDuration): Future[Unit] = producer.close(timeout)

      def close(): Future[Unit] = producer.close()
    }
  }
}
