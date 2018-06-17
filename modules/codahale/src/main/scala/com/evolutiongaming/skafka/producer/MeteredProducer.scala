package com.evolutiongaming.skafka.producer

import com.codahale.metrics.MetricRegistry
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.skafka.ToBytes
import com.evolutiongaming.util.MetricHelper._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object MeteredProducer {

  def apply(producer: Producer, registry: MetricRegistry): Producer = {

    implicit val ec = CurrentThreadExecutionContext

    new Producer {

      def doApply[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

        val topic = record.topic
        val result = registry.histogram(s"$topic.latency").timeFuture {
          producer.doApply(record)
        }

        result.onComplete {
          case Success(metadata) =>
            registry.meter(s"$topic.bytes").mark(metadata.serializedValueSize.toLong)
            registry.counter(s"$topic.success").inc()

          case Failure(_) =>
            registry.counter(s"$topic.failure").inc()
        }

        result
      }

      def flush(): Future[Unit] = producer.flush()

      def closeAsync(timeout: FiniteDuration): Future[Unit] = producer.closeAsync(timeout)

      def close(): Unit = producer.close()
    }
  }
}
