package com.evolutiongaming.skafka.consumer

import cats.Order
import cats.implicits._
import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.skafka._


final case class ConsumerRecord[K, +V](
  topicPartition: TopicPartition,
  offset: Offset,
  timestampAndType: Option[TimestampAndType],
  key: Option[WithSize[K]] = None,
  value: Option[WithSize[V]] = None,
  headers: List[Header] = Nil
) {

  def topic: Topic = topicPartition.topic

  def partition: Partition = topicPartition.partition
}

object ConsumerRecord {
  
  implicit def orderConsumerRecord[K: Order, V]: Order[ConsumerRecord[K, V]] = Order.whenEqual(
    Order.by { a: ConsumerRecord[K, V] => a.topicPartition },
    Order.by { a: ConsumerRecord[K, V] => a.key })
}


final case class ConsumerRecords[K, +V](values: Map[TopicPartition, Nel[ConsumerRecord[K, V]]])

object ConsumerRecords {
  private val _empty = ConsumerRecords(Map.empty)

  def empty[K, V]: ConsumerRecords[K, V] = _empty.asInstanceOf[ConsumerRecords[K, V]]
}


final case class WithSize[+A](value: A, serializedSize: Int = 0)

object WithSize {
  implicit def orderWithSize[A: Order]: Order[WithSize[A]] = Order.by { _.value }
}