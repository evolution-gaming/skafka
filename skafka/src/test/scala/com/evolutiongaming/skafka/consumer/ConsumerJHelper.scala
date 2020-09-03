package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern
import java.util.{ConcurrentModificationException, Collection => CollectionJ, Map => MapJ, Set => SetJ}

import cats.syntax.all._
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, OffsetCommitCallback, Consumer => ConsumerJ, OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

object ConsumerJHelper {

  implicit class ConsumerJOps[K, V](val self: ConsumerJ[K, V]) extends AnyVal {

    def errorOnConcurrentAccess: ConsumerJ[K, V] = {
      val threadIdRef = new AtomicReference(none[Long])
      val lock = new ByName {
        def apply[A](f: => A) = {
          threadIdRef
            .get()
            .fold {
              threadIdRef.set(Thread.currentThread().getId.some)
            } { _ =>
              throw new ConcurrentModificationException("Consumer is not safe for multi-threaded access");
            }

          try f finally threadIdRef.set(none)
        }
      }
      self.map(lock)
    }

    
    def map(f: ByName): ConsumerJ[K, V] = new ConsumerJ[K, V] {

      def assignment() = f { self.assignment() }

      def subscription() = f { self.subscription() }

      def subscribe(topics: CollectionJ[String]) = f { self.subscribe(topics) }

      def subscribe(topics: CollectionJ[String], listener: ConsumerRebalanceListener) = {
        f { self.subscribe(topics, listener) }
      }

      def assign(partitions: CollectionJ[TopicPartitionJ]) = f { self.assign(partitions) }

      def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener) = {
        f { self.subscribe(pattern, callback) }
      }

      def subscribe(pattern: Pattern) = f { self.subscribe(pattern) }

      def unsubscribe() = f { self.unsubscribe() }

      def poll(timeout: Long) = f { self.poll(timeout) }

      def poll(timeout: Duration) = f { self.poll(timeout) }

      def commitSync() = f { self.commitSync() }

      def commitSync(timeout: Duration) = f { self.commitSync(timeout) }

      def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ]) = {
        f { self.commitSync(offsets) }
      }

      def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], timeout: Duration) = {
        f { self.commitSync(offsets, timeout) }
      }

      def commitAsync() = f { self.commitAsync() }

      def commitAsync(callback: OffsetCommitCallback) = {
        f { self.commitAsync(callback) }
      }

      def commitAsync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], callback: OffsetCommitCallback) = {
        f { self.commitAsync(offsets, callback) }
      }

      def seek(partition: TopicPartitionJ, offset: Long) = {
        f { self.seek(partition, offset) }
      }

      def seek(partition: TopicPartitionJ, offsetAndMetadata: OffsetAndMetadataJ) = {
        f { self.seek(partition, offsetAndMetadata) }
      }

      def seekToBeginning(partitions: CollectionJ[TopicPartitionJ]) = {
        f { self.seekToBeginning(partitions) }
      }

      def seekToEnd(partitions: CollectionJ[TopicPartitionJ]) = {
        f { self.seekToEnd(partitions) }
      }

      def position(partition: TopicPartitionJ) = f { self.position(partition) }

      def position(partition: TopicPartitionJ, timeout: Duration) = {
        f { self.position(partition, timeout) }
      }

      def committed(partition: TopicPartitionJ) = {
        f { self.committed(partition) }
      }

      def committed(partition: TopicPartitionJ, timeout: Duration) = {
        f { self.committed(partition, timeout) }
      }

      def committed(partitions: SetJ[TopicPartitionJ]) = {
        f { self.committed(partitions) }
      }

      def committed(partitions: SetJ[TopicPartitionJ], timeout: Duration) = {
        f { self.committed(partitions, timeout) }
      }

      def metrics() = f { self.metrics() }

      def partitionsFor(topic: String) = f { self.partitionsFor(topic) }

      def partitionsFor(topic: String, timeout: Duration) = f { self.partitionsFor(topic, timeout) }

      def listTopics() = f { self.listTopics() }

      def listTopics(timeout: Duration) = f { self.listTopics(timeout) }

      def paused() = f { self.paused() }

      def pause(partitions: CollectionJ[TopicPartitionJ]) = f { self.pause(partitions) }

      def resume(partitions: CollectionJ[TopicPartitionJ]) = f { self.resume(partitions) }

      def offsetsForTimes(timestampsToSearch: MapJ[TopicPartitionJ, LongJ]) = {
        f { self.offsetsForTimes(timestampsToSearch) }
      }

      def offsetsForTimes(timestampsToSearch: MapJ[TopicPartitionJ, LongJ], timeout: Duration) = {
        f { self.offsetsForTimes(timestampsToSearch, timeout) }
      }

      def beginningOffsets(partitions: CollectionJ[TopicPartitionJ]) = {
        f { self.beginningOffsets(partitions) }
      }

      def beginningOffsets(partitions: CollectionJ[TopicPartitionJ], timeout: Duration) = {
        f { self.beginningOffsets(partitions, timeout) }
      }

      def endOffsets(partitions: CollectionJ[TopicPartitionJ]) = {
        f { self.endOffsets(partitions) }
      }

      def endOffsets(partitions: CollectionJ[TopicPartitionJ], timeout: Duration) = {
        f { self.endOffsets(partitions, timeout) }
      }

      def groupMetadata() = f { self.groupMetadata() }

      def close() = f { self.close() }

      def close(timeout: Long, unit: TimeUnit) = f { self.close(timeout, unit) }

      def close(timeout: Duration) = f { self.close(timeout) }

      def wakeup() = f { self.wakeup() }
    }
  }


  trait ByName {
    def apply[A](f: => A): A
  }
}
