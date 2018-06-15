package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.{Header, Partition, Timestamp, Topic}

case class ProducerRecord[+K, +V](
  topic: Topic,
  value: V,
  key: Option[K] = None,
  partition: Option[Partition] = None,
  timestamp: Option[Timestamp] = None,
  headers: List[Header] = Nil)
