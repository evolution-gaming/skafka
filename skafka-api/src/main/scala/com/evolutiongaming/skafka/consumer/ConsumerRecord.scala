package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka._


final case class ConsumerRecord[K, V](
  topicPartition: TopicPartition,
  offset: Offset,
  timestampAndType: Option[TimestampAndType],
  key: Option[WithSize[K]] = None,
  value: Option[WithSize[V]] = None,
  headers: List[Header] = Nil) {

  def topic: Topic = topicPartition.topic

  def partition: Partition = topicPartition.partition
}


final case class ConsumerRecords[K, V](values: Map[TopicPartition, Vector[ConsumerRecord[K, V]]])

object ConsumerRecords {
  private val Empty = ConsumerRecords(Map.empty)

  def empty[K, V]: ConsumerRecords[K, V] = Empty.asInstanceOf[ConsumerRecords[K, V]]
}


final case class WithSize[+T](value: T, serializedSize: Int)