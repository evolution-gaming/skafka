package com.evolutiongaming.skafka

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TopicPartitionSpec extends AnyFunSuite with Matchers {

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
