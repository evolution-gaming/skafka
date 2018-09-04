package com.evolutiongaming.skafka.producer

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.ToBytes

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object LoggingProducer {

  def apply(producer: Producer, log: ActorLog): Producer = {
    implicit val ec = CurrentThreadExecutionContext

    new Producer {
      def send[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

        val result = producer.send(record)(valueToBytes, keyToBytes)
        result.onComplete {
          case Success(metadata) =>
            log.debug(s"sent $record, metadata: $metadata")

          case Failure(failure) =>
            log.error(s"failed to send record $record: $failure", failure)
        }
        result
      }
      def flush(): Future[Unit] = producer.flush()
      def close(): Future[Unit] = producer.close()
      def close(timeout: FiniteDuration): Future[Unit] = producer.close(timeout)
    }
  }
}
