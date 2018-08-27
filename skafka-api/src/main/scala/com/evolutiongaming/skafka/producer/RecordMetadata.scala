package com.evolutiongaming.skafka.producer

import java.time.Instant

import com.evolutiongaming.skafka._

final case class RecordMetadata(
  topicPartition: TopicPartition,
  timestamp: Option[Instant] = None,
  offset: Option[Offset] = None,
  keySerializedSize: Option[Int] = None,
  valueSerializedSize: Option[Int] = None) {

  def topic: Topic = topicPartition.topic

  def partition: Partition = topicPartition.partition
}