package com.evolutiongaming.skafka.consumer

import java.lang.Long as LongJ
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern
import java.util.{ConcurrentModificationException, OptionalLong, Collection as CollectionJ, Map as MapJ, Set as SetJ}
import cats.implicits.*
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{
  CloseOptions,
  ConsumerRebalanceListener,
  OffsetCommitCallback,
  SubscriptionPattern,
  Consumer as ConsumerJ,
  OffsetAndMetadata as OffsetAndMetadataJ
}
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, Uuid, TopicPartition as TopicPartitionJ}

import java.util

object ConsumerJHelper {

  implicit class ConsumerJOps[K, V](val self: ConsumerJ[K, V]) extends AnyVal {

    def errorOnConcurrentAccess: ConsumerJ[K, V] = {
      val threadIdRef = new AtomicReference(none[Long])
      val lock        = new ByName {
        def apply[A](f: => A): A = {
          threadIdRef
            .get()
            .fold {
              threadIdRef.set(Thread.currentThread().getId.some)
            } { _ =>
              throw new ConcurrentModificationException("Consumer is not safe for multi-threaded access");
            }

          try f
          finally threadIdRef.set(none)
        }
      }
      self.map(lock)
    }

    def map(f: ByName): ConsumerJ[K, V] = new ConsumerJ[K, V] {

      def assignment(): SetJ[TopicPartitionJ] = f { self.assignment() }

      def subscription(): SetJ[String] = f { self.subscription() }

      def subscribe(topics: CollectionJ[String]): Unit = f { self.subscribe(topics) }

      def subscribe(topics: CollectionJ[String], listener: ConsumerRebalanceListener): Unit = {
        f { self.subscribe(topics, listener) }
      }

      def assign(partitions: CollectionJ[TopicPartitionJ]): Unit = f { self.assign(partitions) }

      def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): Unit = {
        f { self.subscribe(pattern, callback) }
      }

      def subscribe(pattern: Pattern): Unit = f { self.subscribe(pattern) }

      def subscribe(pattern: SubscriptionPattern, callback: ConsumerRebalanceListener): Unit = {
        f { self.subscribe(pattern, callback) }
      }

      def subscribe(pattern: SubscriptionPattern): Unit = f { self.subscribe(pattern) }

      def unsubscribe(): Unit = f { self.unsubscribe() }

      def poll(timeout: Duration): consumer.ConsumerRecords[K, V] = f { self.poll(timeout) }

      def commitSync(): Unit = f { self.commitSync() }

      def commitSync(timeout: Duration): Unit = f { self.commitSync(timeout) }

      def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ]): Unit = {
        f { self.commitSync(offsets) }
      }

      def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], timeout: Duration): Unit = {
        f { self.commitSync(offsets, timeout) }
      }

      def commitAsync(): Unit = f { self.commitAsync() }

      def commitAsync(callback: OffsetCommitCallback): Unit = {
        f { self.commitAsync(callback) }
      }

      def commitAsync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], callback: OffsetCommitCallback): Unit = {
        f { self.commitAsync(offsets, callback) }
      }

      def registerMetricForSubscription(metric: KafkaMetric): Unit = {
        f { self.registerMetricForSubscription(metric) }
      }

      def unregisterMetricFromSubscription(metric: KafkaMetric): Unit = {
        f { self.unregisterMetricFromSubscription(metric) }
      }

      def seek(partition: TopicPartitionJ, offset: Long): Unit = {
        f { self.seek(partition, offset) }
      }

      def seek(partition: TopicPartitionJ, offsetAndMetadata: OffsetAndMetadataJ): Unit = {
        f { self.seek(partition, offsetAndMetadata) }
      }

      def seekToBeginning(partitions: CollectionJ[TopicPartitionJ]): Unit = {
        f { self.seekToBeginning(partitions) }
      }

      def seekToEnd(partitions: CollectionJ[TopicPartitionJ]): Unit = {
        f { self.seekToEnd(partitions) }
      }

      def position(partition: TopicPartitionJ): Long = f { self.position(partition) }

      def position(partition: TopicPartitionJ, timeout: Duration): Long = {
        f { self.position(partition, timeout) }
      }

      def committed(partitions: SetJ[TopicPartitionJ]): MapJ[TopicPartitionJ, OffsetAndMetadataJ] = {
        f { self.committed(partitions) }
      }

      def committed(partitions: SetJ[TopicPartitionJ], timeout: Duration): MapJ[TopicPartitionJ, OffsetAndMetadataJ] = {
        f { self.committed(partitions, timeout) }
      }

      def clientInstanceId(timeout: Duration): Uuid = f { self.clientInstanceId(timeout) }

      def metrics(): MapJ[MetricName, ? <: Metric] = f { self.metrics() }

      def partitionsFor(topic: String): util.List[PartitionInfo] = f { self.partitionsFor(topic) }

      def partitionsFor(topic: String, timeout: Duration): util.List[PartitionInfo] = f {
        self.partitionsFor(topic, timeout)
      }

      def listTopics(): MapJ[String, util.List[PartitionInfo]] = f { self.listTopics() }

      def listTopics(timeout: Duration): MapJ[String, util.List[PartitionInfo]] = f { self.listTopics(timeout) }

      def paused(): SetJ[TopicPartitionJ] = f { self.paused() }

      def pause(partitions: CollectionJ[TopicPartitionJ]): Unit = f { self.pause(partitions) }

      def resume(partitions: CollectionJ[TopicPartitionJ]): Unit = f { self.resume(partitions) }

      def offsetsForTimes(
        timestampsToSearch: MapJ[TopicPartitionJ, LongJ]
      ): MapJ[TopicPartitionJ, consumer.OffsetAndTimestamp] = {
        f { self.offsetsForTimes(timestampsToSearch) }
      }

      def offsetsForTimes(
        timestampsToSearch: MapJ[TopicPartitionJ, LongJ],
        timeout: Duration
      ): MapJ[TopicPartitionJ, consumer.OffsetAndTimestamp] = {
        f { self.offsetsForTimes(timestampsToSearch, timeout) }
      }

      def beginningOffsets(partitions: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = {
        f { self.beginningOffsets(partitions) }
      }

      def beginningOffsets(
        partitions: CollectionJ[TopicPartitionJ],
        timeout: Duration
      ): MapJ[TopicPartitionJ, LongJ] = {
        f { self.beginningOffsets(partitions, timeout) }
      }

      def endOffsets(partitions: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = {
        f { self.endOffsets(partitions) }
      }

      def endOffsets(partitions: CollectionJ[TopicPartitionJ], timeout: Duration): MapJ[TopicPartitionJ, LongJ] = {
        f { self.endOffsets(partitions, timeout) }
      }

      def groupMetadata(): consumer.ConsumerGroupMetadata = f { self.groupMetadata() }

      def enforceRebalance(): Unit = {}

      def close(): Unit = f { self.close() }

      def close(timeout: Duration): Unit = f { self.close(timeout) }

      def close(option: CloseOptions): Unit = f { self.close(option) }

      def wakeup(): Unit = f { self.wakeup() }

      def currentLag(topicPartition: TopicPartitionJ): OptionalLong = f { self.currentLag(topicPartition) }

      def enforceRebalance(reason: String): Unit = f { self.enforceRebalance(reason) }
    }
  }

  trait ByName {
    def apply[A](f: => A): A
  }
}
