package com.evolutiongaming.skafka.producer

import java.time.Instant

import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Header, TopicPartition}
import org.scalatest.{Matchers, WordSpec}

class ProducerConvertersSpec extends WordSpec with Matchers {

  "ProducerConverters" should {

    "convert Producer.Record" in {
      val record1 = ProducerRecord[Int, String](topic = "topic", value = Some("value"))
      record1.asJava.asScala shouldEqual record1

      val record2 = ProducerRecord[Int, String](
        topic = "topic",
        value = Some("value"),
        key = Some(1),
        partition = Some(2),
        timestamp = Some(Instant.now()),
        headers = List(Header("key", Array[Byte](1, 2, 3))))
      record2.asJava.asScala shouldEqual record2
    }

    "convert RecordMetadata" in {
      val topicPartition = TopicPartition("topic", 1)
      val metadata1 = RecordMetadata(topicPartition)
      metadata1 shouldEqual metadata1.asJava.asScala

      val metadata2 = RecordMetadata(topicPartition, Some(Instant.now()), Some(1), Some(10), Some(100))
      metadata2 shouldEqual metadata2.asJava.asScala
    }
  }
}
