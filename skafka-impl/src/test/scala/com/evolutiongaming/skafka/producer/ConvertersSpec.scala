package com.evolutiongaming.skafka.producer

import java.time.Instant

import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.producer.Producer.RecordMetadata
import org.scalatest.{Matchers, WordSpec}

class ConvertersSpec extends WordSpec with Matchers {

  "Converters" should {

    "convert Header" in {
      val bytes = Array[Byte](1, 2, 3)
      val header = Header("key", bytes)
      header shouldEqual header.asJava.asScala
    }

    "convert RecordMetadata" in {
      val metadata1 = RecordMetadata("topic", 1)
      metadata1 shouldEqual metadata1.asJava.asScala

      val metadata2 = RecordMetadata("topic", 1, Some(Instant.now().toEpochMilli), Some(1), 10, 100)
      metadata2 shouldEqual metadata2.asJava.asScala
    }
  }
}
