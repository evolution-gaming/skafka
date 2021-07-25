package com.evolutiongaming.skafka.consumer

import cats.{Functor, Order}
import cats.implicits._
import com.evolutiongaming.skafka._

final case class ConsumerRecord[K, +V](
  topicPartition: TopicPartition,
  offset: Offset,
  timestampAndType: Option[TimestampAndType],
  key: Option[WithSize[K]]   = None,
  value: Option[WithSize[V]] = None,
  headers: List[Header]      = Nil
) {

  def topic: Topic = topicPartition.topic

  def partition: Partition = topicPartition.partition
}

object ConsumerRecord {

  implicit def orderConsumerRecord[K: Order, V]: Order[ConsumerRecord[K, V]] = {
    Order.whenEqual(
      Order.whenEqual(
        Order.by { a: ConsumerRecord[K, V] => a.topicPartition },
        Order.by { a: ConsumerRecord[K, V] => a.key }
      ),
      Order.by { a: ConsumerRecord[K, V] => a.offset }
    )
  }

  implicit def functorConsumerRecord[K]: Functor[ConsumerRecord[K, *]] = new Functor[ConsumerRecord[K, *]] {
    def map[A, B](fa: ConsumerRecord[K, A])(f: A => B) = {
      fa.copy(value = fa.value.map { _.map(f) })
    }
  }
}
