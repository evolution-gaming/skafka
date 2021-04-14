package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.{
  Consumer => ConsumerJ,
  ConsumerGroupMetadata => ConsumerGroupMetadataJ,
  OffsetAndMetadata => OffsetAndMetadataJ,
  OffsetAndTimestamp => OffsetAndTimestampJ
}
import org.apache.kafka.common.{PartitionInfo => PartitionInfoJ, TopicPartition => TopicPartitionJ}

/**
  * Internal wrapper for [[org.apache.kafka.clients.consumer.Consumer]]
  * with a smaller scope of methods making sense during consumer group rebalance.
  * Introduced in https://github.com/evolution-gaming/skafka/pull/122
  * At the moment of writing we had KafkaConsumer v2.5.0
  * and made following choice about methods
  *  - allowed prefixed with `+ ` in the list below
  *  - not allowed methods prefixed with `- ` in the list below
  *
  *  The choice is based on following factors
  *  - it's ok to use any read-only methods like `assignment`, `position`
  *  - it doesn't make sense to call `consumer.poll` in the middle of current `consumer.poll`
  *  - it's ok to use `commitSync` as we have to do it in a blocking-way inside corresponding method of [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]]
  *  - it doesn't make sense to use `commitAsync` - as we would need to wait for the confirmation before exiting `ConsumerRebalanceListener` method
  *  - we don't want to allow changing current subscription as we're in the middle of `consumer.poll` method
  *  - we don't need to `close` or `wakeup` the consumer, instead we want to gracefully finish the work or start one, and close the consumer after exiting the `poll` method
  *  - we didn't see any use cases for `pause`/`resume` methods, hence they are unsupported
  *  - seek methods are allowed as they are an official way to manipulate consumer position and used as an example in documentation for `ConsumerRebalanceListener`
  * {{{
  * - assign
  * + assignment
  * + beginningOffsets
  * - close
  * - commitAsync
  * + commitSync
  * + committed
  * + endOffsets
  * + groupMetadata
  * + listTopics
  * - metrics
  * + offsetsForTimes
  * + partitionsFor
  * - pause
  * + paused
  * - poll
  * + position
  * - resume
  * + seek
  * + seekToBeginning
  * + seekToEnd
  * - subscribe
  * + subscription
  * - unsubscribe
  * - wakeup
  * }}}
  *
  * If you want to support more methods, please double check kafka documentation and implementation about
  * consumer group rebalance protocol.
  */
private[consumer] trait RebalanceConsumerJ {

  def assignment(): util.Set[TopicPartitionJ]

  def subscription(): util.Set[String]

  def commitSync(): Unit

  def commitSync(timeout: Duration): Unit

  def commitSync(offsets: util.Map[TopicPartitionJ, OffsetAndMetadataJ]): Unit

  def commitSync(offsets: util.Map[TopicPartitionJ, OffsetAndMetadataJ], timeout: Duration): Unit

  def seek(partition: TopicPartitionJ, offset: LongJ): Unit

  def seek(partition: TopicPartitionJ, offsetAndMetadata: OffsetAndMetadataJ): Unit

  def seekToBeginning(partitions: util.Collection[TopicPartitionJ]): Unit

  def seekToEnd(partitions: util.Collection[TopicPartitionJ]): Unit

  def position(partition: TopicPartitionJ): LongJ

  def position(partition: TopicPartitionJ, timeout: Duration): LongJ

  def committed(partitions: util.Set[TopicPartitionJ]): util.Map[TopicPartitionJ, OffsetAndMetadataJ]

  def committed(partitions: util.Set[TopicPartitionJ], timeout: Duration): util.Map[TopicPartitionJ, OffsetAndMetadataJ]

  def partitionsFor(topic: String): util.List[PartitionInfoJ]

  def partitionsFor(topic: String, timeout: Duration): util.List[PartitionInfoJ]

  def listTopics(): util.Map[String, util.List[PartitionInfoJ]]

  def listTopics(timeout: Duration): util.Map[String, util.List[PartitionInfoJ]]

  def paused(): util.Set[TopicPartitionJ]

  def offsetsForTimes(
    timestampsToSearch: util.Map[TopicPartitionJ, LongJ]
  ): util.Map[TopicPartitionJ, OffsetAndTimestampJ]

  def offsetsForTimes(
    timestampsToSearch: util.Map[TopicPartitionJ, LongJ],
    timeout: Duration
  ): util.Map[TopicPartitionJ, OffsetAndTimestampJ]

  def beginningOffsets(partitions: util.Collection[TopicPartitionJ]): util.Map[TopicPartitionJ, LongJ]

  def beginningOffsets(
    partitions: util.Collection[TopicPartitionJ],
    timeout: Duration
  ): util.Map[TopicPartitionJ, LongJ]

  def endOffsets(partitions: util.Collection[TopicPartitionJ]): util.Map[TopicPartitionJ, LongJ]

  def endOffsets(partitions: util.Collection[TopicPartitionJ], timeout: Duration): util.Map[TopicPartitionJ, LongJ]

  def groupMetadata(): ConsumerGroupMetadataJ

}

object RebalanceConsumerJ {
  def apply(c: ConsumerJ[_, _]): RebalanceConsumerJ = new RebalanceConsumerJ {

    def assignment(): util.Set[TopicPartitionJ] =
      c.assignment()

    def subscription(): util.Set[String] =
      c.subscription()

    def commitSync(): Unit =
      c.commitSync()

    def commitSync(timeout: Duration): Unit =
      c.commitSync(timeout)

    def commitSync(offsets: util.Map[TopicPartitionJ, OffsetAndMetadataJ]): Unit =
      c.commitSync(offsets)

    def commitSync(offsets: util.Map[TopicPartitionJ, OffsetAndMetadataJ], timeout: Duration): Unit =
      c.commitSync(offsets, timeout)

    def seek(partition: TopicPartitionJ, offset: LongJ): Unit =
      c.seek(partition, offset)

    def seek(partition: TopicPartitionJ, offsetAndMetadata: OffsetAndMetadataJ): Unit =
      c.seek(partition, offsetAndMetadata)

    def seekToBeginning(partitions: util.Collection[TopicPartitionJ]): Unit =
      c.seekToBeginning(partitions)

    def seekToEnd(partitions: util.Collection[TopicPartitionJ]): Unit =
      c.seekToEnd(partitions)

    def position(partition: TopicPartitionJ): LongJ =
      c.position(partition)

    def position(partition: TopicPartitionJ, timeout: Duration): LongJ =
      c.position(partition, timeout)

    def committed(partitions: util.Set[TopicPartitionJ]): util.Map[TopicPartitionJ, OffsetAndMetadataJ] =
      c.committed(partitions)

    def committed(
      partitions: util.Set[TopicPartitionJ],
      timeout: Duration
    ): util.Map[TopicPartitionJ, OffsetAndMetadataJ] =
      c.committed(partitions, timeout)

    def partitionsFor(topic: String): util.List[PartitionInfoJ] =
      c.partitionsFor(topic)

    def partitionsFor(topic: String, timeout: Duration): util.List[PartitionInfoJ] =
      c.partitionsFor(topic, timeout)

    def listTopics(): util.Map[String, util.List[PartitionInfoJ]] =
      c.listTopics()

    def listTopics(timeout: Duration): util.Map[String, util.List[PartitionInfoJ]] =
      c.listTopics(timeout)

    def paused(): util.Set[TopicPartitionJ] =
      c.paused()

    def offsetsForTimes(
      timestampsToSearch: util.Map[TopicPartitionJ, LongJ]
    ): util.Map[TopicPartitionJ, OffsetAndTimestampJ] =
      c.offsetsForTimes(timestampsToSearch)

    def offsetsForTimes(
      timestampsToSearch: util.Map[TopicPartitionJ, LongJ],
      timeout: Duration
    ): util.Map[TopicPartitionJ, OffsetAndTimestampJ] =
      c.offsetsForTimes(timestampsToSearch, timeout)

    def beginningOffsets(partitions: util.Collection[TopicPartitionJ]): util.Map[TopicPartitionJ, LongJ] =
      c.beginningOffsets(partitions)

    def beginningOffsets(
      partitions: util.Collection[TopicPartitionJ],
      timeout: Duration
    ): util.Map[TopicPartitionJ, LongJ] =
      c.beginningOffsets(partitions, timeout)

    def endOffsets(partitions: util.Collection[TopicPartitionJ]): util.Map[TopicPartitionJ, LongJ] =
      c.endOffsets(partitions)

    def endOffsets(partitions: util.Collection[TopicPartitionJ], timeout: Duration): util.Map[TopicPartitionJ, LongJ] =
      c.endOffsets(partitions, timeout)

    def groupMetadata(): ConsumerGroupMetadataJ =
      c.groupMetadata()
  }
}
