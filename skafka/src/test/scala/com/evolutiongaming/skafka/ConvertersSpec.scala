package com.evolutiongaming.skafka

import cats.Id
import cats.syntax.all._
import com.evolutiongaming.skafka.Converters._
import org.apache.kafka.common.Node
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try


class ConvertersSpec extends AnyWordSpec with Matchers {

  "Converters" should {

    "convert Header" in {
      val bytes = Array[Byte](1, 2, 3)
      val header = Header("key", bytes)
      header shouldEqual header.asJava.asScala
    }

    "convert TopicPartition" in {
      val value = TopicPartition("topic", Partition.min)
      value.pure[Try] shouldEqual value.asJava.asScala[Try]
    }

    "convert PartitionInfo" in {
      val topicPartition = TopicPartition("topic", Partition.min)
      val node = new Node(1, "host", 2)
      val value = PartitionInfo(topicPartition, node, List(node), List(node), List(node))
      value.pure[Try] shouldEqual value.asJava.asScala[Try]
    }

    "convert ToBytes" in {
      val serializer = ToBytes.bytesToBytes[Id].asJava
      serializer.serialize("topic", Bytes.empty) shouldEqual Bytes.empty
      serializer.asScala[Id].apply(Bytes.empty, "topic") shouldEqual Bytes.empty
    }

    "convert FromBytes" in {
      val deserializer = FromBytes.bytesFromBytes[Id].asJava
      deserializer.deserialize("topic", Bytes.empty) shouldEqual Bytes.empty
      deserializer.asScala[Id].apply(Bytes.empty, "topic") shouldEqual Bytes.empty
    }
  }
}
