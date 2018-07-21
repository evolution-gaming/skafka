package com.evolutiongaming.skafka

case class TopicPartition(topic: Topic, partition: Partition) {

  override def toString: Metadata = s"$topic-$partition"
}