package com.evolutiongaming.skafka.consumer

import java.time.Instant
import java.time.temporal.ChronoUnit

import cats.data.NonEmptyList as Nel
import cats.implicits.*
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.Converters.*
import com.evolutiongaming.skafka.consumer.ConsumerConverters.*
import org.apache.kafka.clients.consumer.{
  ConsumerRecord as ConsumerRecordJ,
  ConsumerRecords as ConsumerRecordsJ,
  OffsetAndMetadata as OffsetAndMetadataJ
}
import org.apache.kafka.common.TopicPartition as TopicPartitionJ
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.*
import scala.util.{Success, Try}

class ConsumerConvertersSpec extends AnyWordSpec with Matchers {

  val instant: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  private def record(topic: Topic, partition: Int, offset: Long): ConsumerRecord[String, String] = ConsumerRecord(
    topicPartition   = TopicPartition(topic, Partition.unsafe(partition)),
    offset           = Offset.unsafe(offset),
    timestampAndType = Some(TimestampAndType(instant, TimestampType.create)),
    key              = Some(WithSize("key", 3)),
    value            = Some(WithSize(s"value-$offset", 7)),
    headers          = List(Header("header", Bytes.empty))
  )

  private def consumerRecordsJ(
    records: Map[TopicPartitionJ, List[ConsumerRecordJ[String, String]]]
  ): ConsumerRecordsJ[String, String] = {
    new ConsumerRecordsJ[String, String](
      records.map { case (partitionJ, records) => (partitionJ, records.asJava) }.asJava,
      Map.empty[TopicPartitionJ, OffsetAndMetadataJ].asJava
    )
  }

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
        Some(TimestampAndType(instant, TimestampType.create)),
        Some(TimestampAndType(instant, TimestampType.append))
      )
      key   <- List(Some(WithSize("key", 1)), None)
      value <- List(Some(WithSize("value", 1)), None)
    } {
      s"convert ConsumerRecord, key: $key, value: $value, timestampAndType: $timestampAndType" in {
        val consumerRecord = ConsumerRecord(
          topicPartition   = TopicPartition("topic", Partition.min),
          offset           = Offset.min,
          timestampAndType = timestampAndType,
          key              = key,
          value            = value,
          headers          = List(Header("key", Bytes.empty))
        )
        consumerRecord.pure[Try] shouldEqual consumerRecord.asJava.asScala[Try]
      }
    }

    "convert ConsumerRecords grouped by partition in offset order" in {
      val expected = Map(
        TopicPartition("topic-a", Partition.unsafe(0)) -> Nel.of(record("topic-a", 0, 1), record("topic-a", 0, 2)),
        TopicPartition("topic-a", Partition.unsafe(1)) -> Nel.of(record("topic-a", 1, 5)),
        TopicPartition("topic-b", Partition.unsafe(0)) -> Nel.of(record("topic-b", 0, 3))
      )
      val recordsJ = consumerRecordsJ(expected.map {
        case (topicPartition, records) => (topicPartition.asJava, records.toList.map(_.asJava))
      })

      recordsJ.asScala[Try] shouldEqual Success(ConsumerRecords(expected))
    }

    "group records listed under another partition by their own partition, as built by hand" in {
      val partitionA0 = TopicPartition("topic-a", Partition.unsafe(0))
      val partitionA1 = TopicPartition("topic-a", Partition.unsafe(1))
      val recordsJ    = consumerRecordsJ(
        Map(
          partitionA0.asJava -> List(record("topic-a", 0, 1), record("topic-a", 1, 5), record("topic-a", 0, 2))
            .map(_.asJava)
        )
      )
      val expected = Map(
        partitionA0 -> Nel.of(record("topic-a", 0, 1), record("topic-a", 0, 2)),
        partitionA1 -> Nel.of(record("topic-a", 1, 5))
      )

      recordsJ.asScala[Try] shouldEqual Success(ConsumerRecords(expected))
    }

    "convert empty ConsumerRecords" in {
      val recordsJ = consumerRecordsJ(Map.empty)

      recordsJ.asScala[Try] shouldEqual Success(ConsumerRecords.empty[String, String])
    }

    "fail to convert ConsumerRecords with an invalid offset" in {
      val recordJ  = new ConsumerRecordJ[String, String]("topic", 0, -2L, "key", "value")
      val recordsJ = consumerRecordsJ(Map(new TopicPartitionJ("topic", 0) -> List(recordJ)))

      recordsJ.asScala[Try].failed.get shouldBe a[SkafkaError]
    }

    "timestampsToSearchJ" in {
      timestampsToSearchJ(DataPoints.timeStampsToSearchMap.s) shouldEqual DataPoints.timeStampsToSearchMap.j
    }
  }
}
