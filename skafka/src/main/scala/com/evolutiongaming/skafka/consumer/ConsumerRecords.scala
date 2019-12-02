package com.evolutiongaming.skafka.consumer

import cats.Show
import cats.implicits._
import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.skafka._


final case class ConsumerRecords[K, +V](values: Map[TopicPartition, Nel[ConsumerRecord[K, V]]])

object ConsumerRecords {
  private val _empty = ConsumerRecords(Map.empty)

  def empty[K, V]: ConsumerRecords[K, V] = _empty.asInstanceOf[ConsumerRecords[K, V]]


  val summaryShow: Show[ConsumerRecords[_, _]] = (records: ConsumerRecords[_, _]) => {
    val result = for {
      (topicPartition, records) <- records.values
    } yield {
      val count = records.size
      val from = records.foldLeft(Offset.min) { _ min _.offset }
      val to = records.foldLeft(Offset.min) { _ max _.offset }
      val offset = if (from == to) from else s"$from..$to"
      s"$topicPartition:$offset records: $count"
    }
    result.mkString(", ")
  }
}