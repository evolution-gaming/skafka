package com.evolutiongaming.skafka.producer

import java.time.Instant

import com.evolutiongaming.skafka.{Header, Partition, Topic}

case class ProducerRecord[+K, +V](
  topic: Topic,
  value: V,
  key: Option[K] = None,
  partition: Option[Partition] = None,
  timestamp: Option[Instant] = None,
  headers: List[Header] = Nil)
