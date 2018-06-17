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
      def doApply[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {

        val result = producer.doApply(record)(valueToBytes, keyToBytes)
        result onComplete {
          case Success(metadata) =>
            def msg = record.key match {
              case Some(key) => s"sent record key: $key, metadata: $metadata"
              case None      => s"sent record metadata: $metadata"
            }
            log.debug(msg)

          case Failure(failure) =>
            log.error(s"failed to send record $record: $failure", failure)
        }
        result
      }
      def flush(): Future[Unit] = producer.flush()
      def close(): Unit = producer.close()
      def closeAsync(timeout: FiniteDuration): Future[Unit] = producer.closeAsync(timeout)
    }
  }
}
