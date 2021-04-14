package com.evolutiongaming.skafka.consumer

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.{lang, util}

import com.evolutiongaming.skafka.consumer.RebalanceConsumerJSpec._
import org.apache.kafka.clients.consumer.{
  ConsumerRebalanceListener,
  MockConsumer,
  OffsetAndMetadata,
  OffsetCommitCallback,
  OffsetResetStrategy,
  Consumer => ConsumerJ
}
import org.apache.kafka.common.TopicPartition
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.util.control.NoStackTrace

class RebalanceConsumerJSpec extends AnyFreeSpec with Matchers {

  "compilation failed? check if new method(s) from KafkaConsumer should be supported by RebalanceConsumerJ" in {

    val rebalanceConsumerJ = RebalanceConsumerJ(new MockConsumer[String, String](OffsetResetStrategy.NONE))

    val consumerJ = new ConsumerJ[String, String] {
      def assignment()                                                                    = rebalanceConsumerJ.assignment()
      def subscription()                                                                  = rebalanceConsumerJ.subscription()
      def subscribe(topics: util.Collection[String])                                      = unsupported
      def subscribe(topics: util.Collection[String], callback: ConsumerRebalanceListener) = unsupported
      def assign(partitions: util.Collection[TopicPartition])                             = unsupported
      def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener)                = unsupported
      def subscribe(pattern: Pattern)                                                     = unsupported
      def unsubscribe()                                                                   = unsupported
      def poll(timeout: Long)                                                             = unsupported
      def poll(timeout: Duration)                                                         = unsupported
      def commitSync()                                                                    = rebalanceConsumerJ.commitSync()
      def commitSync(timeout: Duration)                                                   = rebalanceConsumerJ.commitSync(timeout)
      def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata])                = rebalanceConsumerJ.commitSync(offsets)
      def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata], timeout: Duration) =
        rebalanceConsumerJ.commitSync(offsets, timeout)
      def commitAsync()                               = unsupported
      def commitAsync(callback: OffsetCommitCallback) = unsupported
      def commitAsync(offsets: util.Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback) =
        unsupported
      def seek(partition: TopicPartition, offset: Long) = rebalanceConsumerJ.seek(partition, offset)
      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) =
        rebalanceConsumerJ.seek(partition, offsetAndMetadata)
      def seekToBeginning(partitions: util.Collection[TopicPartition]) = rebalanceConsumerJ.seekToBeginning(partitions)
      def seekToEnd(partitions: util.Collection[TopicPartition])       = rebalanceConsumerJ.seekToEnd(partitions)
      def position(partition: TopicPartition)                          = rebalanceConsumerJ.position(partition)
      def position(partition: TopicPartition, timeout: Duration)       = rebalanceConsumerJ.position(partition, timeout)
      def committed(partition: TopicPartition)                         = unsupported
      def committed(partition: TopicPartition, timeout: Duration)      = unsupported
      def committed(partitions: util.Set[TopicPartition])              = rebalanceConsumerJ.committed(partitions)
      def committed(partitions: util.Set[TopicPartition], timeout: Duration) =
        rebalanceConsumerJ.committed(partitions, timeout)
      def metrics()                                           = unsupported
      def partitionsFor(topic: String)                        = rebalanceConsumerJ.partitionsFor(topic)
      def partitionsFor(topic: String, timeout: Duration)     = rebalanceConsumerJ.partitionsFor(topic, timeout)
      def listTopics()                                        = rebalanceConsumerJ.listTopics()
      def listTopics(timeout: Duration)                       = rebalanceConsumerJ.listTopics(timeout)
      def paused()                                            = rebalanceConsumerJ.paused()
      def pause(partitions: util.Collection[TopicPartition])  = unsupported
      def resume(partitions: util.Collection[TopicPartition]) = unsupported
      def offsetsForTimes(timestampsToSearch: util.Map[TopicPartition, lang.Long]) =
        rebalanceConsumerJ.offsetsForTimes(timestampsToSearch)
      def offsetsForTimes(timestampsToSearch: util.Map[TopicPartition, lang.Long], timeout: Duration) =
        rebalanceConsumerJ.offsetsForTimes(timestampsToSearch, timeout)
      def beginningOffsets(partitions: util.Collection[TopicPartition]) =
        rebalanceConsumerJ.beginningOffsets(partitions)
      def beginningOffsets(partitions: util.Collection[TopicPartition], timeout: Duration) =
        rebalanceConsumerJ.beginningOffsets(partitions, timeout)
      def endOffsets(partitions: util.Collection[TopicPartition]) = rebalanceConsumerJ.endOffsets(partitions)
      def endOffsets(partitions: util.Collection[TopicPartition], timeout: Duration) =
        rebalanceConsumerJ.endOffsets(partitions, timeout)
      def groupMetadata()                      = rebalanceConsumerJ.groupMetadata()
      def close()                              = unsupported
      def close(timeout: Long, unit: TimeUnit) = unsupported
      def close(timeout: Duration)             = unsupported
      def wakeup()                             = unsupported
    }

    // useless test to suppress unused consumerJ warning
    consumerJ mustBe consumerJ
  }

}

object RebalanceConsumerJSpec {
  def unsupported: Nothing = throw Unsupported
  case object Unsupported extends NoStackTrace
}
