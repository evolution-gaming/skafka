package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.{Duration => DurationJ}
import java.util.{Optional, List => ListJ, Map => MapJ, Set => SetJ}

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import org.apache.kafka.clients.consumer.{
  ConsumerGroupMetadata => ConsumerGroupMetadataJ,
  OffsetAndMetadata => OffsetAndMetadataJ
}
import org.apache.kafka.common.{Node, PartitionInfo => PartitionInfoJ, TopicPartition => TopicPartitionJ}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object DataPoints {

  final case class JavaScala[J, S](j: J, s: S)

  val partitions: JavaScala[SetJ[TopicPartitionJ], Nes[TopicPartition]] = JavaScala(
    Set(
      new TopicPartitionJ("topic", 3),
      new TopicPartitionJ("topicc", 42)
    ).asJava,
    Nes.of(
      TopicPartition("topic", Partition.unsafe(3)),
      TopicPartition("topicc", Partition.unsafe(42))
    )
  )

  val offsetsMap: JavaScala[MapJ[TopicPartitionJ, LongJ], Nem[TopicPartition, Offset]] = JavaScala(
    Set(
      new TopicPartitionJ("topic", 3)   -> LongJ.valueOf(39L),
      new TopicPartitionJ("topicc", 42) -> LongJ.valueOf(71L)
    ).toMap.asJava,
    Nes
      .of(
        TopicPartition("topic", Partition.unsafe(3))   -> Offset.unsafe(39),
        TopicPartition("topicc", Partition.unsafe(42)) -> Offset.unsafe(71)
      )
      .toNonEmptyList
      .toNem
  )

  val offsetsAndMetadataMap
    : JavaScala[MapJ[TopicPartitionJ, OffsetAndMetadataJ], Nem[TopicPartition, OffsetAndMetadata]] =
    JavaScala(
      asOffsetsAndMetadataJ(offsetsMap.s.map(o => OffsetAndMetadata(o))),
      offsetsMap.s.map(o => OffsetAndMetadata(o))
    )

  val consumerGroupMetadata: JavaScala[ConsumerGroupMetadataJ, ConsumerGroupMetadata] =
    JavaScala(
      new ConsumerGroupMetadataJ(
        "groupId",
        123,
        "memberId",
        Optional.of("groupInstanceId")
      ),
      ConsumerGroupMetadata(
        "groupId",
        123,
        "memberId",
        Some("groupInstanceId")
      )
    )

  val partitionInfo: JavaScala[PartitionInfoJ, PartitionInfo] = {
    val topicPartition = TopicPartition("topic", Partition.min)
    val node           = new Node(1, "host", 2)
    val value          = PartitionInfo(topicPartition, node, List(node), List(node), List(node))
    JavaScala(
      value.asJava,
      value
    )
  }

  val partitionInfoList: JavaScala[ListJ[PartitionInfoJ], List[PartitionInfo]] = JavaScala(
    ListJ.of[PartitionInfoJ](partitionInfo.j),
    List(partitionInfo.s)
  )

  val partitionInfoMap: JavaScala[MapJ[String, ListJ[PartitionInfoJ]], Map[Topic, List[PartitionInfo]]] = JavaScala(
    MapJ.of(partitionInfo.j.topic(), partitionInfoList.j),
    Map(partitionInfo.s.topic -> partitionInfoList.s)
  )

  val timeouts: JavaScala[DurationJ, FiniteDuration] = JavaScala(
    DurationJ.ofSeconds(7),
    7.seconds
  )

}
