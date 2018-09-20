package com.evolutiongaming.skafka

final case class TopicPartition(topic: Topic, partition: Partition) {

  override def toString = s"$topic-$partition"
}

object TopicPartition {
  val Empty: TopicPartition = TopicPartition("", Partition.Min)
}