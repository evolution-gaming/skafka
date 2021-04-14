package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.temporal.ChronoUnit
import java.time.{Instant, Duration => DurationJ}
import java.util.{Optional, List => ListJ, Map => MapJ, Set => SetJ}

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters.OffsetAndTimestampOps
import org.apache.kafka.clients.consumer.{
  ConsumerGroupMetadata => ConsumerGroupMetadataJ,
  OffsetAndMetadata => OffsetAndMetadataJ
}
import org.apache.kafka.common.{Node, PartitionInfo => PartitionInfoJ, TopicPartition => TopicPartitionJ}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object DataPoints {

  final case class JavaScala[J, S](j: J, s: S)

  val partition1 = {
    val topicName = "topic"
    val partition = Partition.unsafe(3)
    JavaScala(
      new TopicPartitionJ(topicName, partition.value),
      TopicPartition(topicName, partition)
    )
  }

  val partition2 = {
    val topicName = "topic2"
    val partition = Partition.unsafe(42)
    JavaScala(
      new TopicPartitionJ(topicName, partition.value),
      TopicPartition(topicName, partition)
    )
  }

  val partitions: JavaScala[SetJ[TopicPartitionJ], Nes[TopicPartition]] = JavaScala(
    Set(
      partition1.j,
      partition2.j
    ).asJava,
    Nes.of(
      partition1.s,
      partition2.s
    )
  )

  val offsetsMap: JavaScala[MapJ[TopicPartitionJ, LongJ], Nem[TopicPartition, Offset]] = JavaScala(
    Set(
      partition1.j -> LongJ.valueOf(39L),
      partition2.j -> LongJ.valueOf(71L)
    ).toMap.asJava,
    Nes
      .of(
        partition1.s -> Offset.unsafe(39),
        partition2.s -> Offset.unsafe(71)
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

  val (timeStampsToSearchMap, offsetsForTimesResponse) = {
    // truncation to millis is needed as we operate with millis precision in transform functions (asJava/asScala)
    val instant1           = Instant.now().truncatedTo(ChronoUnit.MILLIS)
    val instant2           = instant1.plusSeconds(31)
    val timestamp1         = instant1.toEpochMilli
    val timestamp2         = instant2.toEpochMilli
    val offsetAndTimestamp = OffsetAndTimestamp(Offset.unsafe(23), instant1)
    (
      JavaScala(
        MapJ.of(partition1.j, timestamp1, partition2.j, timestamp2),
        Nem.of(
          partition1.s -> instant1,
          partition2.s -> instant2,
        )
      ),
      JavaScala(
        // MapJ.of(...) does not allow `null` values/keys, so using scala Map instead
        Map(partition1.j -> offsetAndTimestamp.asJava, partition2.j -> null).asJavaMap(identity, identity),
        Nem.of(
          partition1.s -> Some(offsetAndTimestamp),
          partition2.s -> Option.empty,
        )
      )
    )
  }

  val timeouts: JavaScala[DurationJ, FiniteDuration] = JavaScala(
    DurationJ.ofSeconds(7),
    7.seconds
  )

}
