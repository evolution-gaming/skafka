package com.evolutiongaming.skafka.producer

import cats.MonadError
import cats.implicits._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.{OffsetAndMetadata, ToBytes, Topic, TopicPartition}

object ProducerLogging {

  type MonadThrowable[F[_]] = MonadError[F, Throwable]
  

  def apply[F[_] : MonadThrowable](producer: Producer[F], log: Log[F]): Producer[F] = {

    new Producer[F] {

      val initTransactions = producer.initTransactions

      val beginTransaction = producer.beginTransaction

      def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        producer.sendOffsetsToTransaction(offsets, consumerGroupId)
      }

      val commitTransaction = producer.commitTransaction

      val abortTransaction = producer.abortTransaction

      def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata] = {
        producer.send(record).attempt.flatMap {
          case Right(metadata) =>
            log.debug(s"sent $record, metadata: $metadata").as(metadata)
          case Left(failure)   =>
            log.error(s"failed to send record $record: $failure") *> failure.raiseError[F, RecordMetadata]
        }
      }

      def partitions(topic: Topic) = producer.partitions(topic)

      val flush = producer.flush
    }
  }
}

