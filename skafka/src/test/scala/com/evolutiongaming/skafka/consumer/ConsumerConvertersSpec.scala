package com.evolutiongaming.skafka.consumer

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.implicits._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try


class ConsumerConvertersSpec extends AnyWordSpec with Matchers {

  val instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  "ConsumerConverters" should {

    "convert OffsetAndMetadata" in {
      val value = OffsetAndMetadata(1L, "metadata")
      value shouldEqual value.asJava.asScala
    }

    "convert OffsetAndTimestamp" in {
      val value = OffsetAndTimestamp(1L, instant)
      value shouldEqual value.asJava.asScala
    }

    for {
      timestampAndType <- List(
        None,
        Some(TimestampAndType(instant, TimestampType.Create)),
        Some(TimestampAndType(instant, TimestampType.Append)))
      key <- List(Some(WithSize("key", 1)), None)
      value <- List(Some(WithSize("value", 1)), None)
    } {
      s"convert ConsumerRecord, key: $key, value: $value, timestampAndType: $timestampAndType" in {
        val consumerRecord = ConsumerRecord(
          topicPartition = TopicPartition("topic", Partition.min),
          offset = 100,
          timestampAndType = timestampAndType,
          key = key,
          value = value,
          headers = List(Header("key", Bytes.empty)))
        consumerRecord.pure[Try] shouldEqual consumerRecord.asJava.asScala[Try]
      }
    }
  }
}
