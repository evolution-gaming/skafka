package com.evolutiongaming.skafka.producer

import cats.implicits._
import com.evolutiongaming.catshelper.{Log, MonadThrowable}
import com.evolutiongaming.skafka.{OffsetAndMetadata, ToBytes, Topic, TopicPartition}

object ProducerLogging {

  def apply[F[_] : MonadThrowable](producer: Producer[F], log: Log[F]): Producer[F] = {

    new Producer[F] {

      val initTransactions = producer.initTransactions

      val beginTransaction = producer.beginTransaction

      def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        producer.sendOffsetsToTransaction(offsets, consumerGroupId)
      }

      val commitTransaction = producer.commitTransaction

      val abortTransaction = producer.abortTransaction

      def send[K, V](
        record: ProducerRecord[K, V])(implicit
        toBytesK: ToBytes[F, K],
        toBytesV: ToBytes[F, V]
      ) = {
        val a = for {
          a <- producer.send(record)
        } yield for {
          a <- a.attempt
          _ <- a match {
            case Right(a) => log.debug(s"sent $record, metadata: $a")
            case Left(e)  => log.error(s"failed to send record $record: $e")
          }
          a <- a.liftTo[F]
        } yield a

        a.handleErrorWith { e =>
          for {
            _ <- log.error(s"failed to send record $record: $e")
            a <- e.raiseError[F, F[RecordMetadata]]
          } yield a
        }
      }

      def partitions(topic: Topic) = producer.partitions(topic)

      val flush = producer.flush
    }
  }
}

