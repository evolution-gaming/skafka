package com.evolutiongaming.skafka.producer

import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.skafka._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait Producer extends Producer.Send {
  def flush(): Future[Unit]
  def close(): Future[Unit]
  def close(timeout: FiniteDuration): Future[Unit]
}

object Producer {

  lazy val Empty: Producer = new Producer {

    def doApply[K, V](record: ProducerRecord[K, V])
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

  trait Send {
    def apply[K, V](record: ProducerRecord[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

      doApply(record)(valueToBytes, keyToBytes)
    }

    def apply[V](record: ProducerRecord[Nothing, V])
      (implicit valueToBytes: ToBytes[V]): Future[RecordMetadata] = {

      doApply(record)(valueToBytes, ToBytes.empty)
    }

    def doApply[K, V](record: ProducerRecord[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata]
  }
}