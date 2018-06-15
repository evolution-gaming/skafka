package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka._

case class RecordMetadata(
  topicPartition: TopicPartition,
  timestamp: Option[Timestamp] = None,
  offset: Option[Offset] = None,
  serializedKeySize: Int = 0,
  serializedValueSize: Int = 0) {

  def topic: Topic = topicPartition.topic

  def partition: Partition = topicPartition.partition
}