package com.evolutiongaming.skafka.consumer

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.{lang, util}

import com.evolutiongaming.skafka.consumer.RebalanceConsumerSpec._
import org.apache.kafka.clients.consumer.{
  ConsumerRebalanceListener,
  OffsetAndMetadata,
  OffsetCommitCallback,
  Consumer => ConsumerJ
}
import org.apache.kafka.common.TopicPartition
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.util.control.NoStackTrace

class RebalanceConsumerSpec extends AnyFreeSpec with Matchers {

  "compilation failed? check if new method(s) from KafkaConsumer should be supported by RebalanceConsumer" in {

//    val rebalanceConsumer = RebalanceConsumer(new MockConsumer[String, String](OffsetResetStrategy.NONE))

    val consumerJ = new ConsumerJ[String, String] {
      def assignment()                               = supported // rebalanceConsumer.assignment()
      def subscription()                             = supported // rebalanceConsumer.subscription()
      def subscribe(topics: util.Collection[String]) = unsupported
      def subscribe(topics: util.Collection[String], callback: ConsumerRebalanceListener) = unsupported
      def assign(partitions: util.Collection[TopicPartition])                             = unsupported
      def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener)                = unsupported
      def subscribe(pattern: Pattern)                                                     = unsupported
      def unsubscribe()                                                                   = unsupported
      def poll(timeout: Long)                                                             = unsupported
      def poll(timeout: Duration)                                                         = unsupported
      def commitSync()                  = supported // rebalanceConsumer.commit()
      def commitSync(timeout: Duration) = supported // rebalanceConsumer.commit(timeout)
      def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]) =
        supported // rebalanceConsumer.commit(offsets)
      def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata], timeout: Duration) =
        supported // rebalanceConsumer.commit(offsets, timeout)
      def commitAsync()                               = unsupported
      def commitAsync(callback: OffsetCommitCallback) = unsupported
      def commitAsync(offsets: util.Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback) =
        unsupported
      def seek(partition: TopicPartition, offset: Long) = supported // rebalanceConsumer.seek(partition, offset)
      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) =
        supported // rebalanceConsumer.seek(partition, offsetAndMetadata)
      def seekToBeginning(partitions: util.Collection[TopicPartition]) =
        supported // rebalanceConsumer.seekToBeginning(partitions)
      def seekToEnd(partitions: util.Collection[TopicPartition]) = supported // rebalanceConsumer.seekToEnd(partitions)
      def position(partition: TopicPartition)                    = supported // rebalanceConsumer.position(partition)
      def position(partition: TopicPartition, timeout: Duration) =
        supported // rebalanceConsumer.position(partition, timeout)
      def committed(partition: TopicPartition)                    = unsupported
      def committed(partition: TopicPartition, timeout: Duration) = unsupported
      def committed(partitions: util.Set[TopicPartition])         = supported // rebalanceConsumer.committed(partitions)
      def committed(partitions: util.Set[TopicPartition], timeout: Duration) =
        supported // rebalanceConsumer.committed(partitions, timeout)
      def metrics()                                       = unsupported
      def partitionsFor(topic: String)                    = supported // rebalanceConsumer.partitionsFor(topic)
      def partitionsFor(topic: String, timeout: Duration) = supported // rebalanceConsumer.partitionsFor(topic, timeout)
      def listTopics()                                    = supported // rebalanceConsumer.topics()
      def listTopics(timeout: Duration)                   = supported // rebalanceConsumer.topics(timeout)
      def paused()                                        = supported // rebalanceConsumer.paused()
      def pause(partitions: util.Collection[TopicPartition])  = unsupported
      def resume(partitions: util.Collection[TopicPartition]) = unsupported
      def offsetsForTimes(timestampsToSearch: util.Map[TopicPartition, lang.Long]) =
        supported // rebalanceConsumer.offsetsForTimes(timestampsToSearch)
      def offsetsForTimes(timestampsToSearch: util.Map[TopicPartition, lang.Long], timeout: Duration) =
        supported // rebalanceConsumer.offsetsForTimes(timestampsToSearch, timeout)
      def beginningOffsets(partitions: util.Collection[TopicPartition]) =
        supported // rebalanceConsumer.beginningOffsets(partitions)
      def beginningOffsets(partitions: util.Collection[TopicPartition], timeout: Duration) =
        supported // rebalanceConsumer.beginningOffsets(partitions, timeout)
      def endOffsets(partitions: util.Collection[TopicPartition]) =
        supported // rebalanceConsumer.endOffsets(partitions)
      def endOffsets(partitions: util.Collection[TopicPartition], timeout: Duration) =
        supported // rebalanceConsumer.endOffsets(partitions, timeout)
      def groupMetadata()                      = supported // rebalanceConsumer.groupMetadata()
      def enforceRebalance()                   = unsupported
      def close()                              = unsupported
      def close(timeout: Long, unit: TimeUnit) = unsupported
      def close(timeout: Duration)             = unsupported
      def wakeup()                             = unsupported
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
