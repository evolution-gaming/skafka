package com.evolutiongaming.skafka

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import org.scalatest.{FunSuite, Matchers}

class TopicPartitionSpec extends FunSuite with Matchers {

  test("show") {
    val topicPartition = TopicPartition(topic = "topic", partition = Partition.Min)
    topicPartition.show shouldEqual "topic-0"
  }

  test("order") {
    Nel.of(
      TopicPartition(topic = "0", partition = 1),
      TopicPartition(topic = "1", partition = 0),
      TopicPartition(topic = "0", partition = 0)
    ).sorted shouldEqual Nel.of(
      TopicPartition(topic = "0", partition = 0),
      TopicPartition(topic = "0", partition = 1),
      TopicPartition(topic = "1", partition = 0))
  }
}
