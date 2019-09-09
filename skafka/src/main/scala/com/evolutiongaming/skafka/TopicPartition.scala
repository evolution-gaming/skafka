package com.evolutiongaming.skafka

import cats.Order
import cats.implicits._

final case class TopicPartition(topic: Topic, partition: Partition) {

  override def toString = s"$topic-$partition"
}

object TopicPartition {

  val empty: TopicPartition = TopicPartition("", Partition.Min)

  implicit val orderTopicPartition: Order[TopicPartition] = Order.whenEqual(
    Order.by { a: TopicPartition => a.topic },
    Order.by { a: TopicPartition => a.partition })
}