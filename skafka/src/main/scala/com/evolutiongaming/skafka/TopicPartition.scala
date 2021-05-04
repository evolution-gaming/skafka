package com.evolutiongaming.skafka

import cats.{Order, Show}
import cats.implicits._

final case class TopicPartition(topic: Topic, partition: Partition) {

  override def toString = s"$topic-$partition"
}

object TopicPartition {

  val empty: TopicPartition = TopicPartition("", Partition.min)

  implicit val orderTopicPartition: Order[TopicPartition] =
    Order.whenEqual(Order.by { a: TopicPartition => a.topic }, Order.by { a: TopicPartition => a.partition })

  implicit val showTopicPartition: Show[TopicPartition] = Show.fromToString
}
