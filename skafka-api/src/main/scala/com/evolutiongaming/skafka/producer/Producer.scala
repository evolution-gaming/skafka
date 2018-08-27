package com.evolutiongaming.skafka.producer

import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.skafka._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait Producer {

  def flush(): Future[Unit]

  def close(): Future[Unit]

  def send[K, V](record: ProducerRecord[K, V])
    (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata]

  def close(timeout: FiniteDuration): Future[Unit]


  def send[V](record: ProducerRecord[Nothing, V])(implicit toBytes: ToBytes[V]): Future[RecordMetadata] = {
    send[Nothing, V](record)(toBytes, ToBytes.empty)
  }

  def send(record: ProducerRecord[Nothing, Nothing]): Future[RecordMetadata] = {
    send[Nothing, Nothing](record)(ToBytes.empty, ToBytes.empty)
  }
}

object Producer {

  lazy val Empty: Producer = new Producer {

    def send[K, V](record: ProducerRecord[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

      val partition = record.partition getOrElse 0
      val topicPartition = TopicPartition(record.topic, partition)
      val metadata = RecordMetadata(topicPartition, record.timestamp)
      metadata.future
    }

    def flush() = Future.unit

    def close() = Future.unit

    def close(timeout: FiniteDuration) = Future.unit
  }


  object Send {
    val Empty: Send = apply(Producer.Empty)

    def apply(producer: Producer): Send = new Send {
      def apply[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {

        producer.send(record)(valueToBytes, keyToBytes)
      }

      def apply[V](record: ProducerRecord[Nothing, V])
        (implicit valueToBytes: ToBytes[V]) = {

        producer.send(record)(valueToBytes)
      }
    }
  }

  trait Send {
    def apply[K, V](record: ProducerRecord[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata]

    def apply[V](record: ProducerRecord[Nothing, V])
      (implicit valueToBytes: ToBytes[V]): Future[RecordMetadata]
  }
}