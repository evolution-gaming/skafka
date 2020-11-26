package com.evolutiongaming.skafka.producer

import java.time.Instant
import java.time.temporal.ChronoUnit
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Header, Offset, Partition, TopicPartition}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ProducerConvertersSpec extends AnyWordSpec with Matchers {

  "ProducerConverters" should {

    val instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)

    "convert Producer.Record" in {
      val record1 = ProducerRecord[Int, String](topic = "topic", value = Some("value"))
      record1.asJava.asScala[Try] shouldEqual record1.pure[Try]

      val record2 = ProducerRecord[Int, String](
        topic = "topic",
        value = Some("value"),
        key = Some(1),
        partition = Some(Partition.min),
        timestamp = Some(instant),
        headers = List(Header("key", Array[Byte](1, 2, 3))))
      record2.asJava.asScala[Try] shouldEqual record2.pure[Try]
    }

    "convert RecordMetadata" in {
      val topicPartition = TopicPartition("topic", Partition.min)
      val metadata1 = RecordMetadata(topicPartition)
      metadata1.pure[Try] shouldEqual metadata1.asJava.asScala[Try]

      val metadata2 = RecordMetadata(topicPartition, Some(instant), Offset.min.some, 10.some, 100.some)
      metadata2.pure[Try] shouldEqual metadata2.asJava.asScala[Try]
    }
  }
}
