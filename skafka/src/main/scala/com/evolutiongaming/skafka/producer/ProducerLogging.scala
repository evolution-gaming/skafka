package com.evolutiongaming.skafka.producer

import cats.implicits._
import cats.MonadThrow
import com.evolutiongaming.catshelper.{Log, MeasureDuration}
import com.evolutiongaming.skafka.{ToBytes, Topic}
import org.apache.kafka.common.errors.RecordTooLargeException

object ProducerLogging {

  private sealed abstract class WithLogging

  def apply[F[_]: MonadThrow: MeasureDuration](producer: Producer[F], log: Log[F]): Producer[F] =
    apply(producer, log, charsToTrim = 1024)

  /**
    * @param charsToTrim a number of chars from record's value to log when producing fails because of a too large record
    */
  def apply[F[_]: MonadThrow: MeasureDuration](producer: Producer[F], log: Log[F], charsToTrim: Int): Producer[F] = {

    new WithLogging with Producer[F] {
      def initTransactions = producer.initTransactions

      def beginTransaction = producer.beginTransaction

      def commitTransaction = producer.commitTransaction

      def abortTransaction = producer.abortTransaction

      def send[K, V](record: ProducerRecord[K, V])(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]) = {
        val a = for {
          d <- MeasureDuration[F].start
          a <- producer.send(record)
        } yield for {
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

      def partitions(topic: Topic) = producer.partitions(topic)

      def flush = producer.flush

      def clientMetrics = producer.clientMetrics

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
    }
  }
}
