package com.evolutiongaming.skafka.producer

import cats.data.NonEmptyMap as Nem
import cats.implicits.*
import cats.MonadThrow
import com.evolutiongaming.catshelper.{Log, MeasureDuration}
import com.evolutiongaming.skafka.{ClientMetric, OffsetAndMetadata, PartitionInfo, ToBytes, Topic, TopicPartition}
import com.evolutiongaming.skafka.consumer.ConsumerGroupMetadata
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.RecordTooLargeException

import scala.concurrent.duration.FiniteDuration

object ProducerLogging {

  private sealed abstract class WithLogging

  def apply[F[_]: MonadThrow: MeasureDuration](producer: Producer[F], log: Log[F]): Producer[F] =
    apply(producer, log, charsToTrim = 1024)

  /** @param charsToTrim
    *   a number of chars from record's value to log when producing fails because of a too large record
    */
  def apply[F[_]: MonadThrow: MeasureDuration](producer: Producer[F], log: Log[F], charsToTrim: Int): Producer[F] = {

    new WithLogging with Producer[F] {
      def initTransactions: F[Unit] = producer.initTransactions

      def beginTransaction: F[Unit] = producer.beginTransaction

      def commitTransaction: F[Unit] = producer.commitTransaction

      def abortTransaction: F[Unit] = producer.abortTransaction

      def sendOffsetsToTransaction(
        offsets: Nem[TopicPartition, OffsetAndMetadata],
        consumerGroupMetadata: ConsumerGroupMetadata
      ): F[Unit] = producer.sendOffsetsToTransaction(offsets, consumerGroupMetadata)

      def send[K, V](
        record: ProducerRecord[K, V]
      )(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]): F[F[RecordMetadata]] = {
        val a = for {
          d <- MeasureDuration[F].start
          a <- producer.send(record)
        } yield
          for {
            a <- a.attempt
            d <- d
            _ <- a match {
              case Right(a) => log.debug(s"send in ${d.toMillis}ms, $record, result: $a")
              case Left(e)  => logError(record, e)
            }
            a <- a.liftTo[F]
          } yield a

        a.handleErrorWith { e =>
          for {
            _ <- logError(record, e)
            a <- e.raiseError[F, F[RecordMetadata]]
          } yield a
        }
      }

      def partitions(topic: Topic): F[List[PartitionInfo]] = producer.partitions(topic)

      def flush: F[Unit] = producer.flush

      def clientMetrics: F[Seq[ClientMetric[F]]] = producer.clientMetrics

      private def logError[K, V](record: ProducerRecord[K, V], err: Throwable): F[Unit] =
        err match {
          case _: RecordTooLargeException =>
            val trimmed = record.value.map(_.toString().take(charsToTrim))

            log.error(
              s"Failed to send too large record, topic: ${record.topic}, " +
                s"partition: ${record.partition}, key: ${record.key}, " +
                s"timestamp: ${record.timestamp}, headers: ${record.headers}, " +
                s"trimmed value (first $charsToTrim chars): $trimmed, error: $err"
            )
          case _ =>
            log.error(s"failed to send record $record: $err")
        }

      def clientInstanceId(timeout: FiniteDuration): F[Uuid] = producer.clientInstanceId(timeout)
    }
  }
}
