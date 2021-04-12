package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.{Duration => DurationJ}
import java.util.concurrent.{TimeUnit => TimeUnitJ}
import java.util.regex.Pattern
import java.util.{Collection => CollectionJ, List => ListJ, Map => MapJ, Set => SetJ}

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{
  ConsumerRebalanceListener,
  OffsetAndMetadata,
  OffsetCommitCallback,
  Consumer => ConsumerJ
}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition => TopicPartitionJ}

/**
  * It is intentional to have all methods as `???` (throws NotImplementedError)
  *
  * It is used to verify the only expected interaction in corresponding tests
  * by implementing the only expected method to be called in test
  */
class ExplodingConsumer extends ConsumerJ[String, String] {
  def assignment(): SetJ[TopicPartitionJ] = ???

  def subscription(): SetJ[String] = ???

  def subscribe(topics: CollectionJ[String]): Unit = ???

  def subscribe(topics: CollectionJ[String], callback: ConsumerRebalanceListener): Unit = ???

  def assign(partitions: CollectionJ[TopicPartitionJ]): Unit = ???

  def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): Unit = ???

  def subscribe(pattern: Pattern): Unit = ???

  def unsubscribe(): Unit = ???

  def poll(timeout: Long): consumer.ConsumerRecords[String, String] = ???

  def poll(timeout: DurationJ): consumer.ConsumerRecords[String, String] = ???

  def commitSync(): Unit = ???

  def commitSync(timeout: DurationJ): Unit = ???

  def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadata]): Unit = ???

  def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadata], timeout: DurationJ): Unit = ???

  def commitAsync(): Unit = ???

  def commitAsync(callback: OffsetCommitCallback): Unit = ???

  def commitAsync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadata], callback: OffsetCommitCallback): Unit = ???

  def seek(partition: TopicPartitionJ, offset: Long): Unit = ???

  def seek(partition: TopicPartitionJ, offsetAndMetadata: OffsetAndMetadata): Unit = ???

  def seekToBeginning(partitions: CollectionJ[TopicPartitionJ]): Unit = ???

  def seekToEnd(partitions: CollectionJ[TopicPartitionJ]): Unit = ???

  def position(partition: TopicPartitionJ): Long = ???

  def position(partition: TopicPartitionJ, timeout: DurationJ): Long = ???

  def committed(partition: TopicPartitionJ): OffsetAndMetadata = ???

  def committed(partition: TopicPartitionJ, timeout: DurationJ): OffsetAndMetadata = ???

  def committed(partitions: SetJ[TopicPartitionJ]): MapJ[TopicPartitionJ, OffsetAndMetadata] = ???

  def committed(
    partitions: SetJ[TopicPartitionJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, OffsetAndMetadata] = ???

  def metrics(): MapJ[MetricName, _ <: Metric] = ???

  def partitionsFor(topic: String): ListJ[PartitionInfo] = ???

  def partitionsFor(topic: String, timeout: DurationJ): ListJ[PartitionInfo] = ???

  def listTopics(): MapJ[String, ListJ[PartitionInfo]] = ???

  def listTopics(timeout: DurationJ): MapJ[String, ListJ[PartitionInfo]] = ???

  def paused(): SetJ[TopicPartitionJ] = ???

  def pause(partitions: CollectionJ[TopicPartitionJ]): Unit = ???

  def resume(partitions: CollectionJ[TopicPartitionJ]): Unit = ???

  def offsetsForTimes(
    timestampsToSearch: MapJ[TopicPartitionJ, LongJ]
  ): MapJ[TopicPartitionJ, consumer.OffsetAndTimestamp] = ???

  def offsetsForTimes(
    timestampsToSearch: MapJ[TopicPartitionJ, LongJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, consumer.OffsetAndTimestamp] = ???

  def beginningOffsets(partitions: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = ???

  def beginningOffsets(
    partitions: CollectionJ[TopicPartitionJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, LongJ] = ???

  def endOffsets(partitions: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = ???

  def endOffsets(
    partitions: CollectionJ[TopicPartitionJ],
    timeout: DurationJ
  ): MapJ[TopicPartitionJ, LongJ] = ???

  def groupMetadata(): consumer.ConsumerGroupMetadata = ???

  def close(): Unit = ???

  def close(timeout: Long, unit: TimeUnitJ): Unit = ???

  def close(timeout: DurationJ): Unit = ???

  def wakeup(): Unit = ???
}
