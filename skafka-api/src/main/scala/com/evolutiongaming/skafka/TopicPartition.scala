package com.evolutiongaming.skafka

final case class TopicPartition(topic: Topic, partition: Partition) {

  override def toString: Metadata = s"$topic-$partition"
}

object TopicPartition {
  val Empty: TopicPartition = TopicPartition("", 0)
}