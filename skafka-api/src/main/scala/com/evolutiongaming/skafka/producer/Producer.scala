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

  sealed trait Send {
    def apply[K, V](record: Record[K, V])
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