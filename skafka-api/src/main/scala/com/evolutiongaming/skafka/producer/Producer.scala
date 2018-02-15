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

    def doApply[K, V](record: Record[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

      val metadata = RecordMetadata(record.topic, record.partition getOrElse 0, record.timestamp)
      Future.successful(metadata)
    }

    def flush(): Future[Unit] = Future.successful(())

    def closeAsync(timeout: FiniteDuration): Future[Unit] = Future.successful(())

    def close(): Unit = Future.successful(())
  }

  trait Send {
    def apply[K, V](record: Record[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

      doApply(record)(valueToBytes, keyToBytes)
    }

    def apply[V](record: Record[Nothing, V])
      (implicit valueToBytes: ToBytes[V]): Future[RecordMetadata] = {

      doApply(record)(valueToBytes, ToBytes.empty)
    }

    protected def doApply[K, V](record: Record[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata]
  }

  case class Record[+K, +V](
    topic: Topic,
    value: V,
    key: Option[K] = None,
    partition: Option[Partition] = None,
    timestamp: Option[Timestamp] = None,
    headers: List[Header] = Nil
  )

  case class RecordMetadata(
    topic: Topic,
    partition: Partition,
    timestamp: Option[Timestamp] = None,
    offset: Option[Offset] = None,
    serializedKeySize: Int = 0,
    serializedValueSize: Int = 0
  )
}