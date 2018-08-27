package com.evolutiongaming.skafka.producer

import java.time.Instant

import com.evolutiongaming.skafka._

final case class ProducerRecord[+K, +V](
  topic: Topic,
  value: Option[V] = None,
  key: Option[K] = None,
  partition: Option[Partition] = None,
  timestamp: Option[Instant] = None,
  headers: List[Header] = Nil) {

  def toBytes(implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): ProducerRecord[Bytes, Bytes] = {
    val valueBytes = value.map { key => valueToBytes(key, topic) }
    val keyBytes = key.map { key => keyToBytes(key, topic) }
    copy(value = valueBytes, key = keyBytes)
  }
}
