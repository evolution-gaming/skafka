package com.evolutiongaming.skafka

import java.lang.{Long => LongJ}

import cats.Id
import cats.data.{NonEmptySet => Nes}
import cats.implicits._
import com.evolutiongaming.skafka.Converters._
import org.apache.kafka.common.{Node, TopicPartition => TopicPartitionJ}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
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

    "asOffsetsAndMetadataJ" in {
      val scala = Nes
        .of(
          TopicPartition("topic", Partition.unsafe(3))   -> OffsetAndMetadata(Offset.unsafe(39), "meta"),
          TopicPartition("topicc", Partition.unsafe(42)) -> OffsetAndMetadata(Offset.unsafe(71), "data")
        )
        .toNonEmptyList
        .toNem
      val expected = Set(
        new TopicPartitionJ("topic", 3)   -> new OffsetAndMetadataJ(39, "meta"),
        new TopicPartitionJ("topicc", 42) -> new OffsetAndMetadataJ(71, "data")
      ).toMap.asJava

      asOffsetsAndMetadataJ(scala) shouldEqual expected
    }
  }
}
