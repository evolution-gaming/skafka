package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConsumerRecordSpec extends AnyFunSuite with Matchers {

  test("order") {

    def consumerRecord(topic: Topic, partition: Int, offset: Offset, key: Int) = {
      ConsumerRecord(
        topicPartition = TopicPartition(topic, Partition.unsafe(partition)),
        offset = offset,
        timestampAndType = none,
        key = Some(WithSize(key)),
        value = none,
        headers = Nil)
    }

    val notSorted = Nel.of(
      consumerRecord(topic = "0", partition = 0, offset = 1, key = 0),
      consumerRecord(topic = "0", partition = 0, offset = 2, key = 1),
      consumerRecord(topic = "0", partition = 0, offset = 4, key = 2),
      consumerRecord(topic = "0", partition = 0, offset = 0, key = 0),
      consumerRecord(topic = "0", partition = 0, offset = 3, key = 0),
      consumerRecord(topic = "0", partition = 1, offset = 1, key = 11),
      consumerRecord(topic = "0", partition = 2, offset = 0, key = 21),
      consumerRecord(topic = "1", partition = 0, offset = 1, key = 0),
      consumerRecord(topic = "1", partition = 0, offset = 0, key = 0),
      consumerRecord(topic = "0", partition = 1, offset = 0, key = 11))

    val expected = Nel.of(
      consumerRecord(topic = "0", partition = 0, offset = 0, key = 0),
      consumerRecord(topic = "0", partition = 0, offset = 1, key = 0),
      consumerRecord(topic = "0", partition = 0, offset = 3, key = 0),
      consumerRecord(topic = "0", partition = 0, offset = 2, key = 1),
      consumerRecord(topic = "0", partition = 0, offset = 4, key = 2),
      consumerRecord(topic = "0", partition = 1, offset = 0, key = 11),
      consumerRecord(topic = "0", partition = 1, offset = 1, key = 11),
      consumerRecord(topic = "0", partition = 2, offset = 0, key = 21),
      consumerRecord(topic = "1", partition = 0, offset = 0, key = 0),
      consumerRecord(topic = "1", partition = 0, offset = 1, key = 0))

    notSorted.sorted shouldEqual expected
  }
}
