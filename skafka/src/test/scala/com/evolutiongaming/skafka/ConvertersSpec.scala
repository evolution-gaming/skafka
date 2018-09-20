package com.evolutiongaming.skafka

import com.evolutiongaming.skafka.Converters._
import org.apache.kafka.common.Node
import org.scalatest.{Matchers, WordSpec}


class ConvertersSpec extends WordSpec with Matchers {

  "Converters" should {

    "convert Header" in {
      val bytes = Array[Byte](1, 2, 3)
      val header = Header("key", bytes)
      header shouldEqual header.asJava.asScala
    }

    "convert TopicPartition" in {
      val value = TopicPartition("topic", 1)
      value shouldEqual value.asJava.asScala
    }

    "convert PartitionInfo" in {
      val topicPartition = TopicPartition("topic", 1)
      val node = new Node(1, "host", 2)
      val value = PartitionInfo(topicPartition, node, List(node), List(node), List(node))
      value shouldEqual value.asJava.asScala
    }

    "convert ToBytes" in {
      val serializer = ToBytes.BytesToBytes.asJava
      serializer.serialize("topic", Bytes.Empty) shouldEqual Bytes.Empty
    }

    "convert FromBytes" in {
      FromBytes.BytesFromBytes.asJava.deserialize("topic", Bytes.Empty) shouldEqual Bytes.Empty
    }
  }
}
