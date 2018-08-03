package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka._


final case class ConsumerRecord[K, V](
  topicPartition: TopicPartition,
  offset: Offset,
  timestampAndType: Option[TimestampAndType],
  serializedKeySize: Int,
  serializedValueSize: Int,
  key: Option[K],
  value: V,
  headers: List[Header]) {

  def topic: Topic = topicPartition.topic

  def partition: Partition = topicPartition.partition
}

final case class ConsumerRecords[K, V](values: Map[TopicPartition, Vector[ConsumerRecord[K, V]]])
