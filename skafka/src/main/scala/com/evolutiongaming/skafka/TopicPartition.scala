package com.evolutiongaming.skafka

final case class TopicPartition(topic: Topic, partition: Partition) {

  override def toString = s"$topic-$partition"
}

object TopicPartition {
  val empty: TopicPartition = TopicPartition("", Partition.Min)
}