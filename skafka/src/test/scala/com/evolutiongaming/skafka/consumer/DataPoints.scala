package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.{Duration => DurationJ}
import java.util.{Map => MapJ, Set => SetJ}

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

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

  val timeouts: JavaScala[DurationJ, FiniteDuration] = JavaScala(
    DurationJ.ofSeconds(7),
    7.seconds
  )

}
