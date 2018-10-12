package com.evolutiongaming.skafka.producer

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.{OffsetAndMetadata, ToBytes, Topic, TopicPartition}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object LoggingProducer {

  def apply(producer: Producer[Future], log: ActorLog): Producer[Future] = {
    implicit val ec = CurrentThreadExecutionContext

    new Producer[Future] {

      def initTransactions() = producer.initTransactions()

      def beginTransaction() = producer.beginTransaction()

      def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        producer.sendOffsetsToTransaction(offsets, consumerGroupId)
      }

      def commitTransaction() = producer.commitTransaction()

      def abortTransaction() = producer.abortTransaction()

      def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): Future[RecordMetadata] = {
        val result = producer.send(record)
        result.onComplete {
          case Success(metadata) =>
            log.debug(s"sent $record, metadata: $metadata")

          case Failure(failure) =>
            log.error(s"failed to send record $record: $failure", failure)
        }
        result
      }

      def partitions(topic: Topic) = producer.partitions(topic)

      def flush() = producer.flush()

      def close() = producer.close()

      def close(timeout: FiniteDuration) = producer.close(timeout)
    }
  }
}
