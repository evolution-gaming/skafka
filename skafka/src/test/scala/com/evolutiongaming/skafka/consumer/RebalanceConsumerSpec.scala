package com.evolutiongaming.skafka.consumer

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.{lang, util}
import java.lang.{Long => LongJ}
import com.evolutiongaming.skafka.consumer.RebalanceConsumerSpec._
import org.apache.kafka.clients.consumer.{
  ConsumerRebalanceListener,
  ConsumerGroupMetadata => ConsumerGroupMetadataJ,
  OffsetAndMetadata,
  OffsetCommitCallback,
  OffsetAndTimestamp => OffsetAndTimestampJ,
  Consumer => ConsumerJ,
  ConsumerRecords => ConsumerRecordsJ
}
import org.apache.kafka.common.{TopicPartition, PartitionInfo, MetricName, Metric}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import java.util.OptionalLong
import scala.util.control.NoStackTrace

class RebalanceConsumerSpec extends AnyFreeSpec with Matchers {

  "compilation failed? check if new method(s) from KafkaConsumer should be supported by RebalanceConsumer" in {

//    val rebalanceConsumer = RebalanceConsumer(new MockConsumer[String, String](OffsetResetStrategy.NONE))

    val consumerJ = new ConsumerJ[String, String] {
      def assignment(): util.Set[TopicPartition]           = supported // rebalanceConsumer.assignment()
      def subscription(): util.Set[String]                 = supported // rebalanceConsumer.subscription()
      def subscribe(topics: util.Collection[String]): Unit = unsupported
      def subscribe(topics: util.Collection[String], callback: ConsumerRebalanceListener): Unit = unsupported
      def assign(partitions: util.Collection[TopicPartition]): Unit                             = unsupported
      def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): Unit                = unsupported
      def subscribe(pattern: Pattern): Unit                                                     = unsupported
      def unsubscribe(): Unit                                                                   = unsupported
      def poll(timeout: Long): ConsumerRecordsJ[String, String]                                 = unsupported
      def poll(timeout: Duration): ConsumerRecordsJ[String, String]                             = unsupported
      def commitSync(): Unit                  = supported // rebalanceConsumer.commit()
      def commitSync(timeout: Duration): Unit = supported // rebalanceConsumer.commit(timeout)
      def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit =
        supported // rebalanceConsumer.commit(offsets)
      def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata], timeout: Duration): Unit =
        supported // rebalanceConsumer.commit(offsets, timeout)
      def commitAsync(): Unit                               = unsupported
      def commitAsync(callback: OffsetCommitCallback): Unit = unsupported
      def commitAsync(offsets: util.Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): Unit =
        unsupported
      def seek(partition: TopicPartition, offset: Long): Unit = supported // rebalanceConsumer.seek(partition, offset)
      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Unit =
        supported // rebalanceConsumer.seek(partition, offsetAndMetadata)
      def seekToBeginning(partitions: util.Collection[TopicPartition]): Unit =
        supported // rebalanceConsumer.seekToBeginning(partitions)
      def seekToEnd(partitions: util.Collection[TopicPartition]): Unit =
        supported // rebalanceConsumer.seekToEnd(partitions)
      def position(partition: TopicPartition): Long = supported // rebalanceConsumer.position(partition)
      def position(partition: TopicPartition, timeout: Duration): Long =
        supported // rebalanceConsumer.position(partition, timeout)
      def committed(partition: TopicPartition): OffsetAndMetadata                                      = unsupported
      def committed(partition: TopicPartition, timeout: Duration): OffsetAndMetadata                   = unsupported
      def committed(partitions: util.Set[TopicPartition]): util.Map[TopicPartition, OffsetAndMetadata] =
        supported // rebalanceConsumer.committed(partitions)
      def committed(
        partitions: util.Set[TopicPartition],
        timeout: Duration
      ): util.Map[TopicPartition, OffsetAndMetadata] =
        supported // rebalanceConsumer.committed(partitions, timeout)
      def metrics(): util.Map[MetricName, _ <: Metric]           = unsupported
      def partitionsFor(topic: String): util.List[PartitionInfo] = supported // rebalanceConsumer.partitionsFor(topic)
      def partitionsFor(topic: String, timeout: Duration): util.List[PartitionInfo] =
        supported // rebalanceConsumer.partitionsFor(topic, timeout)
      def listTopics(): util.Map[String, util.List[PartitionInfo]] = supported // rebalanceConsumer.topics()
      def listTopics(timeout: Duration): util.Map[String, util.List[PartitionInfo]] =
        supported // rebalanceConsumer.topics(timeout)
      def paused(): util.Set[TopicPartition]                        = supported // rebalanceConsumer.paused()
      def pause(partitions: util.Collection[TopicPartition]): Unit  = unsupported
      def resume(partitions: util.Collection[TopicPartition]): Unit = unsupported
      def offsetsForTimes(
        timestampsToSearch: util.Map[TopicPartition, lang.Long]
      ): util.Map[TopicPartition, OffsetAndTimestampJ] =
        supported // rebalanceConsumer.offsetsForTimes(timestampsToSearch)
      def offsetsForTimes(
        timestampsToSearch: util.Map[TopicPartition, lang.Long],
        timeout: Duration
      ): util.Map[TopicPartition, OffsetAndTimestampJ] =
        supported // rebalanceConsumer.offsetsForTimes(timestampsToSearch, timeout)
      def beginningOffsets(partitions: util.Collection[TopicPartition]): util.Map[TopicPartition, LongJ] =
        supported // rebalanceConsumer.beginningOffsets(partitions)
      def beginningOffsets(
        partitions: util.Collection[TopicPartition],
        timeout: Duration
      ): util.Map[TopicPartition, LongJ] =
        supported // rebalanceConsumer.beginningOffsets(partitions, timeout)
      def endOffsets(partitions: util.Collection[TopicPartition]): util.Map[TopicPartition, LongJ] =
        supported // rebalanceConsumer.endOffsets(partitions)
      def endOffsets(partitions: util.Collection[TopicPartition], timeout: Duration): util.Map[TopicPartition, LongJ] =
        supported // rebalanceConsumer.endOffsets(partitions, timeout)
      def groupMetadata(): ConsumerGroupMetadataJ                  = supported // rebalanceConsumer.groupMetadata()
      def enforceRebalance(): Unit                                 = unsupported
      def close(): Unit                                            = unsupported
      def close(timeout: Long, unit: TimeUnit): Unit               = unsupported
      def close(timeout: Duration): Unit                           = unsupported
      def wakeup(): Unit                                           = unsupported
      def currentLag(topicPartition: TopicPartition): OptionalLong = unsupported
      def enforceRebalance(reason: String): Unit                   = unsupported
    }

    // useless test to suppress unused consumerJ warning
    consumerJ mustBe consumerJ
  }

}

object RebalanceConsumerSpec {
  def unsupported: Nothing = throw Unsupported
  def supported: Nothing   = throw Supported

  case object Unsupported extends NoStackTrace
  case object Supported extends NoStackTrace
}
