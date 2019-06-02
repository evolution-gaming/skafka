package com.evolutiongaming.skafka.producer

import cats.implicits._
import cats.{Monad, MonadError}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.{OffsetAndMetadata, ToBytes, Topic, TopicPartition}

import scala.concurrent.duration.FiniteDuration

object LoggingProducer {
  type CanFail[F[_]] = MonadError[F, Throwable]

  def apply[F[_] : CanFail : Monad](producer: Producer[F], log: Log[F]): Producer[F] = {

    new Producer[F] {

      override val initTransactions = producer.initTransactions

      override val beginTransaction = producer.beginTransaction

      override def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        producer.sendOffsetsToTransaction(offsets, consumerGroupId)
      }

      override val commitTransaction = producer.commitTransaction

      override val abortTransaction = producer.abortTransaction

      override def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata] = {
        producer.send(record).attempt.flatMap {
          case Right(metadata) =>
            log.debug(s"sent $record, metadata: $metadata").as(metadata)
          case Left(failure)   =>
            log.error(s"failed to send record $record: $failure") *>
              implicitly[CanFail[F]].raiseError[RecordMetadata](failure)
        }
      }

      override def partitions(topic: Topic) = producer.partitions(topic)

      override val flush = producer.flush

      override val close = producer.close

      override def close(timeout: FiniteDuration) = producer.close(timeout)
    }
  }
}
