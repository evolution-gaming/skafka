package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.producer.ProducerConverters._
import org.scalatest.{Matchers, WordSpec}

import scala.compat.Platform

class ProducerConvertersSpec extends WordSpec with Matchers {

  "ProducerConverters" should {

    "convert Producer.Record" in {
      val record1 = Producer.Record[Int, String](topic = "topic", value = "value")
      record1.asJava.asScala shouldEqual record1

      val record2 = Producer.Record[Int, String](
        topic = "topic",
        value = "value",
        key = Some(1),
        partition = Some(2),
        timestamp = Some(Platform.currentTime),
        headers = List(Header("key", Array[Byte](1, 2, 3))))
      record2.asJava.asScala shouldEqual record2
    }
  }
}
