package com.evolutiongaming.skafka.producer

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.ToBytes

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
            log.debug(s"sent $record, metadata: $metadata")

          case Failure(failure) =>
            log.error(s"failed to send record $record: $failure", failure)
        }
        result
      }
      def flush() = producer.flush()
      def close() = producer.close()
      def close(timeout: FiniteDuration) = producer.close(timeout)
    }
  }
}
