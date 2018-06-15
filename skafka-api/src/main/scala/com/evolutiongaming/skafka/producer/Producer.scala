package com.evolutiongaming.skafka.producer

import java.io.Closeable

import com.evolutiongaming.skafka._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait Producer extends Producer.Send with Closeable {
  def flush(): Future[Unit]
  def close(): Unit // does blocking
  def closeAsync(timeout: FiniteDuration): Future[Unit]
}

object Producer {

  lazy val Empty: Producer = new Producer {

    def doApply[K, V](record: ProducerRecord[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

      val partition = record.partition getOrElse 0
      val topicPartition = TopicPartition(record.topic, partition)
      val metadata = RecordMetadata(topicPartition, record.timestamp)
      Future.successful(metadata)
    }

    def flush(): Future[Unit] = Future.successful(())

    def closeAsync(timeout: FiniteDuration): Future[Unit] = Future.successful(())

    def close(): Unit = Future.successful(())
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