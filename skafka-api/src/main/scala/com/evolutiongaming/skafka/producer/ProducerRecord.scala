package com.evolutiongaming.skafka.producer

import java.time.Instant

import com.evolutiongaming.skafka._

final case class ProducerRecord[+K, +V](
  topic: Topic,
  value: V,
  key: Option[K] = None,
  partition: Option[Partition] = None,
  timestamp: Option[Instant] = None,
  headers: List[Header] = Nil) {

  def toBytes(implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): ProducerRecord[Bytes, Bytes] = {
    val keyBytes = key.map { key => keyToBytes(key, topic) }
    val valueBytes = valueToBytes(value, topic)
    copy(value = valueBytes, key = keyBytes)
  }
}
