package com.evolutiongaming.skafka.consumer

import java.time.Instant

import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import org.scalatest.{Matchers, WordSpec}


class ConsumerConvertersSpec extends WordSpec with Matchers {

  val timestamp = Instant.now()

  "ConsumerConverters" should {

    "convert OffsetAndMetadata" in {
      val value = OffsetAndMetadata(1, "metadata")
      value shouldEqual value.asJava.asScala
    }

    "convert OffsetAndTimestamp" in {
      val value = OffsetAndTimestamp(1, Instant.now())
      value shouldEqual value.asJava.asScala
    }

    for {
      timestampAndType <- List(
        None,
        Some(TimestampAndType(timestamp, TimestampType.Create)),
        Some(TimestampAndType(timestamp, TimestampType.Append)))
      key <- List(Some(WithSize("key", 1)), None)
      value <- List(Some(WithSize("value", 1)), None)
    } {
      s"convert ConsumerRecord, key: $key, value: $value, timestampAndType: $timestampAndType" in {
        val consumerRecord = ConsumerRecord(
          topicPartition = TopicPartition("topic", 1),
          offset = 100,
          timestampAndType = timestampAndType,
          key = key,
          value = value,
          headers = List(Header("key", Bytes.Empty)))
        consumerRecord shouldEqual consumerRecord.asJava.asScala
      }
    }
  }
}
