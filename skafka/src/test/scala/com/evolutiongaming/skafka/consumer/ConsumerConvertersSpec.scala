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

  val instant: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  "ConsumerConverters" should {

    "convert OffsetAndMetadata" in {
      val value = OffsetAndMetadata(Offset.min, "metadata")
      value.pure[Try] shouldEqual value.asJava.asScala[Try]
    }

    "convert OffsetAndTimestamp" in {
      val value = OffsetAndTimestamp(Offset.min, instant)
      value.pure[Try] shouldEqual value.asJava.asScala[Try]
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
          offset = Offset.min,
          timestampAndType = timestampAndType,
          key = key,
          value = value,
          headers = List(Header("key", Bytes.empty)))
        consumerRecord.pure[Try] shouldEqual consumerRecord.asJava.asScala[Try]
      }
    }

    "timestampsToSearchJ" in {
      timestampsToSearchJ(DataPoints.timeStampsToSearchMap.s) shouldEqual DataPoints.timeStampsToSearchMap.j
    }
  }
}
