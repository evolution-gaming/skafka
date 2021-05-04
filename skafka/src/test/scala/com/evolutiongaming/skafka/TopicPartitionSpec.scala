package com.evolutiongaming.skafka

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TopicPartitionSpec extends AnyFunSuite with Matchers {

  test("show") {
    val topicPartition = TopicPartition(topic = "topic", partition = Partition.min)
    topicPartition.show shouldEqual "topic-0"
  }

  test("order") {

    def topicPartition(topic: Topic, partition: Int) = {
      TopicPartition(topic = topic, partition = Partition.unsafe(partition))
    }

    Nel
      .of(
        topicPartition(topic = "0", partition = 1),
        topicPartition(topic = "1", partition = 0),
        topicPartition(topic = "0", partition = 0)
      )
      .sorted shouldEqual Nel.of(
      topicPartition(topic = "0", partition = 0),
      topicPartition(topic = "0", partition = 1),
      topicPartition(topic = "1", partition = 0)
    )
  }
}
