package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.{Duration => DurationJ}
import java.util.{Collection => CollectionJ, List => ListJ, Map => MapJ, Set => SetJ}

import com.evolutiongaming.skafka.consumer.ExplodingConsumer._
import org.apache.kafka.clients.consumer.{
  ConsumerGroupMetadata => ConsumerGroupMetadataJ,
  OffsetAndMetadata => OffsetAndMetadataJ,
  OffsetAndTimestamp => OffsetAndTimestampJ
}
import org.apache.kafka.common.{PartitionInfo, TopicPartition => TopicPartitionJ}

import scala.util.control.NoStackTrace

/**
  * It is intentional to have all methods as `notImplemented` (throws NotImplementedOnPurpose)
  *
  * It is used to verify the only expected interaction in corresponding tests
  * by implementing the only expected method to be called in test
  */
class ExplodingConsumer extends RebalanceConsumerJ {
  def assignment(): SetJ[TopicPartitionJ] = notImplemented

  def subscription(): SetJ[String] = notImplemented

  def commitSync(): Unit = notImplemented

  def commitSync(timeout: DurationJ): Unit = notImplemented

  def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ]): Unit = notImplemented

  def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], timeout: DurationJ): Unit = notImplemented

  def seek(partition: TopicPartitionJ, offset: LongJ): Unit = notImplemented

  def seek(partition: TopicPartitionJ, OffsetAndMetadataJ: OffsetAndMetadataJ): Unit = notImplemented

  def seekToBeginning(partitions: CollectionJ[TopicPartitionJ]): Unit = notImplemented

  def seekToEnd(partitions: CollectionJ[TopicPartitionJ]): Unit = notImplemented

  def position(partition: TopicPartitionJ): LongJ = notImplemented

  def position(partition: TopicPartitionJ, timeout: DurationJ): LongJ = notImplemented

  def committed(partitions: SetJ[TopicPartitionJ]): MapJ[TopicPartitionJ, OffsetAndMetadataJ] = notImplemented

  def committed(
    partitions: SetJ[TopicPartitionJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, OffsetAndMetadataJ] = notImplemented

  def partitionsFor(topic: String): ListJ[PartitionInfo] = notImplemented

  def partitionsFor(topic: String, timeout: DurationJ): ListJ[PartitionInfo] = notImplemented

  def listTopics(): MapJ[String, ListJ[PartitionInfo]] = notImplemented

  def listTopics(timeout: DurationJ): MapJ[String, ListJ[PartitionInfo]] = notImplemented

  def paused(): SetJ[TopicPartitionJ] = notImplemented

  def offsetsForTimes(
    timestampsToSearch: MapJ[TopicPartitionJ, LongJ]
  ): MapJ[TopicPartitionJ, OffsetAndTimestampJ] = notImplemented

  def offsetsForTimes(
    timestampsToSearch: MapJ[TopicPartitionJ, LongJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, OffsetAndTimestampJ] = notImplemented

  def beginningOffsets(partitions: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = notImplemented

  def beginningOffsets(
    partitions: CollectionJ[TopicPartitionJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, LongJ] = notImplemented

  def endOffsets(partitions: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = notImplemented

  def endOffsets(
    partitions: CollectionJ[TopicPartitionJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, LongJ] = notImplemented

  def groupMetadata(): ConsumerGroupMetadataJ = notImplemented

}

object ExplodingConsumer {

  def notImplemented: Nothing = throw NotImplementedOnPurpose

  case object NotImplementedOnPurpose extends NoStackTrace
}
