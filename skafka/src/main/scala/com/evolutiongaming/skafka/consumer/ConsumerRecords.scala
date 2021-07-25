package com.evolutiongaming.skafka.consumer

import cats.{Functor, Show}
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.skafka._

final case class ConsumerRecords[K, +V](values: Map[TopicPartition, Nel[ConsumerRecord[K, V]]])

object ConsumerRecords {
  private val _empty = ConsumerRecords(Map.empty)

  def empty[K, V]: ConsumerRecords[K, V] = _empty.asInstanceOf[ConsumerRecords[K, V]]

  val summaryShow: Show[ConsumerRecords[_, _]] = (records: ConsumerRecords[_, _]) => {
    val result = for {
      (topicPartition, records) <- records.values
    } yield {
      val count  = records.size
      val to     = records.foldLeft(Offset.min) { _ max _.offset }
      val from   = records.foldLeft(to) { _ min _.offset }
      val offset = if (from == to) from else s"$from..$to"
      s"$topicPartition:$offset records: $count"
    }
    result.mkString(", ")
  }

  implicit def functorConsumerRecords[K]: Functor[ConsumerRecords[K, *]] = new Functor[ConsumerRecords[K, *]] {
    def map[A, B](fa: ConsumerRecords[K, A])(f: A => B) = {
      fa.copy(values = fa.values.map { case (key, value) => (key, value.map { _.map(f) }) })
    }
  }
}
