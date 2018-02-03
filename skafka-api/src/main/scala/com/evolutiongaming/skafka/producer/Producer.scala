package com.evolutiongaming.skafka.producer

import java.io.Closeable

import com.evolutiongaming.skafka._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait Producer[K, V] extends Closeable {
  import Producer._

  def send(record: Record[K, V]): Future[RecordMetadata]
  def flush(): Future[Unit]
  def close(): Unit // does blocking
  def closeAsync(timeout: FiniteDuration): Future[Unit]
}

object Producer {

  case class Record[K, V](
    key: K,
    value: V,
    topic: Topic,
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