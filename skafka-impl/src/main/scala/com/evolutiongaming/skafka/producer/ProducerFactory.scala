package com.evolutiongaming.skafka.producer


import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import org.apache.kafka.clients.producer.{Callback, Producer => JProducer, RecordMetadata => JRecordMetadata}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, ExecutionException, Future, Promise}
import scala.util.control.NonFatal

object ProducerFactory {
  import Producer._

  def apply[K, V](producer: JProducer[K, V], blockingEc: ExecutionContext): Producer[K, V] = new Producer[K, V] {

    def send(record: Record[K, V]): Future[RecordMetadata] = {
      val jRecord = record.asJava
      val promise = Promise[RecordMetadata]
      val callback = new Callback {
        def onCompletion(metadata: JRecordMetadata, exception: Exception) = {
          if (metadata == null) promise.failure(exception)
          else promise.success(metadata.asScala)
        }
      }
      try {
        val result = producer.send(jRecord, callback)
        if (result.isDone) {
          val jMetadata = result.get()
          val metadata = jMetadata.asScala
          Future.successful(metadata)
        } else {
          promise.future
        }
      } catch {
        case NonFatal(failure: ExecutionException) => Future.failed(failure.getCause)
        case NonFatal(failure)                     => Future.failed(failure)
      }
    }

    def flush(): Future[Unit] = {
      asyncBlocking {
        producer.flush()
      }
    }

    def close(): Unit = producer.close()

    def closeAsync(timeout: FiniteDuration): Future[Unit] = {
      asyncBlocking {
        producer.close(timeout.length, timeout.unit)
      }
    }

    private def asyncBlocking[T](f: => T): Future[T] = Future(f)(blockingEc)
  }
}
