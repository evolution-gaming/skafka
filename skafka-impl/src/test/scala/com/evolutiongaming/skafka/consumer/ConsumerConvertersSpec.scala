package com.evolutiongaming.skafka.consumer

import java.time.Instant

import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import org.scalatest.{Matchers, WordSpec}


class ConsumerConvertersSpec extends WordSpec with Matchers {

  "ConsumerConverters" should {

    "convert OffsetAndMetadata" in {
      val value = OffsetAndMetadata(1, "metadata")
      value shouldEqual value.asJava.asScala
    }

    "convert OffsetAndTimestamp" in {
      val value = OffsetAndTimestamp(1, Instant.now())
      value shouldEqual value.asJava.asScala
    }

    "convert ConsumerRecord" in {
      val value = ConsumerRecord(
        topicPartition = TopicPartition("topic", 1),
        offset = 100,
        timestampAndType = Some(TimestampAndType(Instant.now(), TimestampType.Create)),
        serializedKeySize = 1,
        serializedValueSize = 2,
        key = Some("key"),
        value = "value",
        headers = Nil)
      value shouldEqual value.asJava.asScala
    }
  }
}
