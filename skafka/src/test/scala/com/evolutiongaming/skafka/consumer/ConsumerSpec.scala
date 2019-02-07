package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.temporal.ChronoUnit
import java.time.{Instant, Duration => DurationJ}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util.{Collection => CollectionJ, Map => MapJ}

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import org.apache.kafka.clients.consumer.{Consumer => ConsumerJ, ConsumerRebalanceListener => ConsumerRebalanceListenerJ, ConsumerRecords => ConsumerRecordsJ, OffsetAndMetadata => OffsetAndMetadataJ, OffsetCommitCallback => OffsetCommitCallbackJ}
import org.apache.kafka.common.{Node, TopicPartition => TopicPartitionJ}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Success

class ConsumerSpec extends WordSpec with Matchers {
  val topic = "topic"
  val partition = 1
  val offset = 2l
  val topicPartition = TopicPartition(topic, partition)
  val offsetAndMetadata = OffsetAndMetadata(offset, "metadata")
  val offsets = Map((topicPartition, offsetAndMetadata))
  val partitions = Nel(topicPartition)
  val instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)
  val offsetAndTimestamp = OffsetAndTimestamp(offset, instant)
  val consumerRecord = ConsumerRecord(
    topicPartition = topicPartition,
    offset = offset,
    timestampAndType = Some(TimestampAndType(instant, TimestampType.Create)),
    key = Some(WithSize(Bytes.Empty, 1)),
    value = Some(WithSize(Bytes.Empty, 1)),
    headers = List(Header("key", Bytes.Empty)))
  val consumerRecords = ConsumerRecords(Map((topicPartition, List(consumerRecord))))

  val node = new Node(1, "host", 2)

  val partitionInfo = PartitionInfo(
    topicPartition,
    leader = node,
    replicas = List(node),
    inSyncReplicas = List(node),
    offlineReplicas = List(node))

  "Consumer" should {

    "assign" in new Scope {
      consumer.assign(partitions)
      assign shouldEqual List(topicPartition.asJava)
    }

    "assignment" in new Scope {
      consumer.assignment() shouldEqual Set(topicPartition)
    }

    "subscribe topics" in new Scope {
      consumer.subscribe(Nel(topic), Some(rebalanceListener))
      subscribeTopics shouldEqual List(topic)
      assigned shouldEqual true
      revoked shouldEqual true
    }

    "subscribe pattern" in new Scope {
      val pattern = Pattern.compile(".")
      consumer.subscribe(pattern, Some(rebalanceListener))
      subscribePattern shouldEqual Some(pattern)
      assigned shouldEqual true
      revoked shouldEqual true
    }

    "subscription" in new Scope {
      consumer.subscription() shouldEqual Set(topic)
    }

    "unsubscribe" in new Scope {
      consumer.unsubscribe().value shouldEqual Some(Success(()))
      unsubscribe shouldEqual true
    }

    "poll" in new Scope {
      consumer.poll(1.second).value shouldEqual Some(Success(consumerRecords))
    }

    "commit" in new Scope {
      consumer.commit().value shouldEqual Some(Success(()))
      commit shouldEqual None
    }

    "commit with timeout" in new Scope {
      consumer.commit(1.second).value shouldEqual Some(Success(()))
      commit shouldEqual Some(1.second)
    }

    "commit offsets" in new Scope {
      consumer.commit(offsets).value shouldEqual Some(Success(()))
      commitSync shouldEqual Some((offsets, None))
    }

    "commit offsets with timeout" in new Scope {
      consumer.commit(offsets, 1.second).value shouldEqual Some(Success(()))
      commitSync shouldEqual Some((offsets, Some(1.second)))
    }

    "commitLater" in new Scope {
      consumer.commitLater().value shouldEqual Some(Success(offsets))
    }

    "commitLater offsets" in new Scope {
      consumer.commitLater(offsets).value shouldEqual Some(Success(()))
      commitLater shouldEqual offsets
    }

    "seek" in new Scope {
      consumer.seek(topicPartition, offset)
      seek shouldEqual Some((topicPartition.asJava, offset))
    }

    "seekToBeginning" in new Scope {
      consumer.seekToBeginning(partitions)
      seekToBeginning shouldEqual List(topicPartition.asJava)
    }

    "seekToEnd" in new Scope {
      consumer.seekToEnd(partitions)
      seekToEnd shouldEqual List(topicPartition.asJava)
    }

    "position" in new Scope {
      consumer.position(topicPartition).value shouldEqual Some(Success(offset))
    }

    "position with timeout" in new Scope {
      consumer.position(topicPartition, 1.second).value shouldEqual Some(Success(offset))
    }

    "committed" in new Scope {
      consumer.committed(topicPartition).value shouldEqual Some(Success(offsetAndMetadata))
    }

    "committed with timeout" in new Scope {
      consumer.committed(topicPartition, 1.second).value shouldEqual Some(Success(offsetAndMetadata))
    }

    "partitions" in new Scope {
      consumer.partitions(topic).value shouldEqual Some(Success(List(partitionInfo)))
    }

    "partitions with timeout" in new Scope {
      consumer.partitions(topic, 1.second).value shouldEqual Some(Success(List(partitionInfo)))
    }

    "listTopics" in new Scope {
      consumer.listTopics().value shouldEqual Some(Success(Map((topic, List(partitionInfo)))))
    }

    "listTopics with timeout" in new Scope {
      consumer.listTopics(1.second).value shouldEqual Some(Success(Map((topic, List(partitionInfo)))))
    }

    "pause" in new Scope {
      consumer.pause(partitions)
      pause shouldEqual List(topicPartition.asJava)
    }

    "paused" in new Scope {
      consumer.paused() shouldEqual Set(topicPartition)
    }

    "resume" in new Scope {
      consumer.resume(partitions)
      resume shouldEqual List(topicPartition.asJava)
    }

    "offsetsForTimes" in new Scope {
      consumer.offsetsForTimes(Map((topicPartition, offset))).value shouldEqual Some(Success(Map((topicPartition, Some(offsetAndTimestamp)))))
    }

    "offsetsForTimes with timeout" in new Scope {
      consumer.offsetsForTimes(Map((topicPartition, offset)), 1.second).value shouldEqual Some(Success(Map((topicPartition, Some(offsetAndTimestamp)))))
    }

    "beginningOffsets" in new Scope {
      consumer.beginningOffsets(partitions).value shouldEqual Some(Success(Map((topicPartition, offset))))
    }

    "beginningOffsets with timeout" in new Scope {
      consumer.beginningOffsets(partitions, 1.second).value shouldEqual Some(Success(Map((topicPartition, offset))))
    }

    "endOffsets" in new Scope {
      consumer.endOffsets(partitions).value shouldEqual Some(Success(Map((topicPartition, offset))))
    }

    "endOffsets with timeout" in new Scope {
      consumer.endOffsets(partitions, 1.second).value shouldEqual Some(Success(Map((topicPartition, offset))))
    }

    "close" in new Scope {
      consumer.close().value shouldEqual Some(Success(()))
      close shouldEqual true
    }

    "close with timeout" in new Scope {
      consumer.close(1.second).value shouldEqual Some(Success(()))
      closeTimeout shouldEqual Some(1.second)
    }

    "wakeup" in new Scope {
      consumer.wakeup().value shouldEqual Some(Success(()))
      wakeup shouldEqual true
    }
  }

  private trait Scope {

    var assign = List.empty[TopicPartitionJ]

    var subscribeTopics = List.empty[Topic]

    var subscribePattern = Option.empty[Pattern]

    var unsubscribe = false

    var commitLater = Map.empty[TopicPartition, OffsetAndMetadata]

    var commit = Option.empty[FiniteDuration]

    var commitSync = Option.empty[(Map[TopicPartition, OffsetAndMetadata], Option[FiniteDuration])]

    var commitSyncTimeout = Option.empty[Duration]

    var close = false

    var closeTimeout = Option.empty[FiniteDuration]

    var pause = List.empty[TopicPartitionJ]

    var resume = List.empty[TopicPartitionJ]

    var wakeup = false

    var seek = Option.empty[(TopicPartitionJ, Offset)]

    var seekToBeginning = List.empty[TopicPartitionJ]

    var seekToEnd = List.empty[TopicPartitionJ]

    var revoked = false

    var assigned = false

    val rebalanceListener = new RebalanceListener {
      def onPartitionsAssigned(partitions: immutable.Iterable[TopicPartition]) = assigned = true
      def onPartitionsRevoked(partitions: immutable.Iterable[TopicPartition]): Unit = revoked = true
    }

    val consumerJ = new ConsumerJ[Bytes, Bytes] {

      def assignment() = Set(topicPartition.asJava).asJava

      def subscription() = Set(topic).asJava

      def subscribe(topics: CollectionJ[String]) = {}

      def subscribe(topics: CollectionJ[String], callback: ConsumerRebalanceListenerJ) = {
        subscribeTopics = topics.asScala.toList
        callback.onPartitionsAssigned(List.empty.asJava)
        callback.onPartitionsRevoked(List.empty.asJava)
      }

      def assign(partitions: CollectionJ[TopicPartitionJ]) = {
        Scope.this.assign = partitions.asScala.toList
      }

      def subscribe(pattern: Pattern, callback: ConsumerRebalanceListenerJ) = {
        subscribePattern = Some(pattern)
        callback.onPartitionsAssigned(List.empty.asJava)
        callback.onPartitionsRevoked(List.empty.asJava)
      }

      def subscribe(pattern: Pattern) = {}

      def unsubscribe() = {
        Scope.this.unsubscribe = true
      }

      def poll(timeout: Long) = {
        val records = Map((topicPartition, List(consumerRecord.asJava).to[ListBuffer])).asJavaMap(_.asJava, _.asJava)
        new ConsumerRecordsJ[Bytes, Bytes](records)
      }

      def poll(timeout: DurationJ) = {
        poll(timeout.toMillis)
      }

      def commitSync() = {
        Scope.this.commit = None
      }

      def commitSync(timeout: DurationJ) = {
        Scope.this.commit = Some(timeout.asScala)
      }

      def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ]) = {
        Scope.this.commitSync = Some((offsets.asScalaMap(_.asScala, _.asScala), None))
      }

      def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], timeout: DurationJ) = {
        Scope.this.commitSync = Some((offsets.asScalaMap(_.asScala, _.asScala), Some(timeout.asScala)))
      }

      def commitAsync() = {}

      def commitAsync(callback: OffsetCommitCallbackJ) = {
        callback.onComplete(offsets.asJavaMap(_.asJava, _.asJava), null)
      }

      def commitAsync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], callback: OffsetCommitCallbackJ) = {
        commitLater = offsets.asScalaMap(_.asScala, _.asScala)
        callback.onComplete(offsets, null)
      }

      def seek(partition: TopicPartitionJ, offset: Long) = {
        Scope.this.seek = Some((partition, offset))
      }

      def seekToBeginning(partitions: CollectionJ[TopicPartitionJ]) = {
        Scope.this.seekToBeginning = partitions.asScala.toList
      }

      def seekToEnd(partitions: CollectionJ[TopicPartitionJ]) = {
        Scope.this.seekToEnd = partitions.asScala.toList
      }

      def position(partition: TopicPartitionJ) = offset

      def position(partition: TopicPartitionJ, timeout: DurationJ) = position(partition)


      def committed(partition: TopicPartitionJ) = offsetAndMetadata.asJava

      def committed(partition: TopicPartitionJ, timeout: DurationJ) = offsetAndMetadata.asJava


      def metrics() = new java.util.HashMap()

      def partitionsFor(topic: Topic) = {
        List(partitionInfo.asJava).to[ListBuffer].asJava
      }

      def partitionsFor(topic: Topic, timeout: DurationJ) = {
        partitionsFor(topic)
      }

      def listTopics() = {
        Map((topic, List(partitionInfo.asJava).to[ListBuffer].asJava)).asJavaMap(x => x, x => x)
      }

      def listTopics(timeout: DurationJ) = {
        listTopics()
      }

      def paused() = Set(topicPartition.asJava).asJava

      def pause(partitions: CollectionJ[TopicPartitionJ]) = {
        Scope.this.pause = partitions.asScala.toList
      }

      def resume(partitions: CollectionJ[TopicPartitionJ]) = {
        Scope.this.resume = partitions.asScala.toList
      }

      def offsetsForTimes(timestampsToSearch: MapJ[TopicPartitionJ, LongJ]) = {
        Map((topicPartition, offsetAndTimestamp)).asJavaMap(_.asJava, _.asJava)
      }

      def offsetsForTimes(timestampsToSearch: MapJ[TopicPartitionJ, LongJ], timeout: DurationJ) = {
        offsetsForTimes(timestampsToSearch)
      }

      def beginningOffsets(partitions: CollectionJ[TopicPartitionJ]) = {
        Map((topicPartition, offset)).asJavaMap(_.asJava, l => l)
      }

      def beginningOffsets(partitions: CollectionJ[TopicPartitionJ], timeout: DurationJ) = {
        beginningOffsets(partitions)
      }

      def endOffsets(partitions: CollectionJ[TopicPartitionJ]) = {
        Map((topicPartition, offset)).asJavaMap(_.asJava, l => l)
      }

      def endOffsets(partitions: CollectionJ[TopicPartitionJ], timeout: DurationJ) = {
        endOffsets(partitions)
      }

      def close() = {
        Scope.this.close = true
      }

      def close(timeout: Long, unit: TimeUnit) = {
        Scope.this.closeTimeout = Some(FiniteDuration(timeout, unit))
      }

      def close(timeout: DurationJ) = {
        Scope.this.closeTimeout = Some(timeout.asScala)
      }

      def wakeup() = {
        Scope.this.wakeup = true
      }
    }

    val consumer = {
      val consumer = Consumer(consumerJ, CurrentThreadExecutionContext)
      Consumer(consumer, Consumer.Metrics.Empty)
    }
  }
}
