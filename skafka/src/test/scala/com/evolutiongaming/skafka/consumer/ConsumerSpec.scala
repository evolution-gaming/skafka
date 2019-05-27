package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.temporal.ChronoUnit
import java.time.{Instant, Duration => DurationJ}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util.{Collection => CollectionJ, Map => MapJ}

import cats.effect.IO
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.IOMatchers._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import org.apache.kafka.clients.consumer.{Consumer => ConsumerJ, ConsumerRebalanceListener => ConsumerRebalanceListenerJ, ConsumerRecords => ConsumerRecordsJ, OffsetAndMetadata => OffsetAndMetadataJ, OffsetCommitCallback => OffsetCommitCallbackJ}
import org.apache.kafka.common.{Node, TopicPartition => TopicPartitionJ}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

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
      verify(consumer.assign(partitions)) { _ =>
        assign shouldEqual List(topicPartition.asJava)
      }
    }

    "assignment" in new Scope {
      consumer.assignment should produce(Set(topicPartition))
    }

    "subscribe topics" in new Scope {
      verify(consumer.subscribe(Nel(topic), Some(rebalanceListener))) { _ =>
        subscribeTopics shouldEqual List(topic)
        assigned shouldEqual true
        revoked shouldEqual true
      }
    }

    "subscribe pattern" in new Scope {
      val pattern = Pattern.compile(".")
      verify(consumer.subscribe(pattern, Some(rebalanceListener))) { _ =>
        subscribePattern shouldEqual Some(pattern)
        assigned shouldEqual true
        revoked shouldEqual true
      }
    }

    "subscription" in new Scope {
      consumer.subscription should produce(Set(topic))
    }

    "unsubscribe" in new Scope {
      verify(consumer.unsubscribe) { _ =>
        unsubscribe shouldEqual true
      }
    }

    "poll" in new Scope {
      consumer.poll(1.second) should produce(consumerRecords)
    }

    "commit" in new Scope {
      verify(consumer.commit) { _ =>
        commit shouldEqual None
      }
    }

    "commit with timeout" in new Scope {
      verify(consumer.commit(1.second)) { _ =>
        commit shouldEqual Some(1.second)
      }
    }

    "commit offsets" in new Scope {
      verify(consumer.commit(offsets)) { _ =>
        commitSync shouldEqual Some((offsets, None))
      }
    }

    "commit offsets with timeout" in new Scope {
      verify(consumer.commit(offsets, 1.second)) { _ =>
        commitSync shouldEqual Some((offsets, Some(1.second)))
      }
    }

    "commitLater" in new Scope {
      consumer.commitLater should produce(offsets)
    }

    "commitLater offsets" in new Scope {
      verify(consumer.commitLater(offsets)) { _ =>
        commitLater shouldEqual offsets
      }
    }

    "seek" in new Scope {
      verify(consumer.seek(topicPartition, offset)) { _ =>
        seek shouldEqual Some((topicPartition.asJava, offset))
      }
    }

    "seekToBeginning" in new Scope {
      verify(consumer.seekToBeginning(partitions)) { _ =>
        seekToBeginning shouldEqual List(topicPartition.asJava)
      }
    }

    "seekToEnd" in new Scope {
      verify(consumer.seekToEnd(partitions)) { _ =>
        seekToEnd shouldEqual List(topicPartition.asJava)
      }
    }

    "position" in new Scope {
      consumer.position(topicPartition) should produce(offset)
    }

    "position with timeout" in new Scope {
      consumer.position(topicPartition, 1.second) should produce(offset)
    }

    "committed" in new Scope {
      consumer.committed(topicPartition) should produce(offsetAndMetadata)
    }

    "committed with timeout" in new Scope {
      consumer.committed(topicPartition, 1.second) should produce(offsetAndMetadata)
    }

    "partitions" in new Scope {
      consumer.partitions(topic) should produce(List(partitionInfo))
    }

    "partitions with timeout" in new Scope {
      consumer.partitions(topic, 1.second) should produce(List(partitionInfo))
    }

    "listTopics" in new Scope {
      consumer.listTopics should produce(Map((topic, List(partitionInfo))))
    }

    "listTopics with timeout" in new Scope {
      consumer.listTopics(1.second) should produce(Map((topic, List(partitionInfo))))
    }

    "pause" in new Scope {
      verify(consumer.pause(partitions)) { _ =>
        pause shouldEqual List(topicPartition.asJava)
      }
    }

    "paused" in new Scope {
      consumer.paused should produce(Set(topicPartition))
    }

    "resume" in new Scope {
      verify(consumer.resume(partitions)) { _ =>
        resume shouldEqual List(topicPartition.asJava)
      }
    }

    "offsetsForTimes" in new Scope {
      consumer.offsetsForTimes(Map((topicPartition, offset))) should produce(Map((topicPartition, Option(offsetAndTimestamp))))
    }

    "offsetsForTimes with timeout" in new Scope {
      consumer.offsetsForTimes(Map((topicPartition, offset)), 1.second) should produce(Map((topicPartition, Option(offsetAndTimestamp))))
    }

    "beginningOffsets" in new Scope {
      consumer.beginningOffsets(partitions) should produce(Map((topicPartition, offset)))
    }

    "beginningOffsets with timeout" in new Scope {
      consumer.beginningOffsets(partitions, 1.second) should produce(Map((topicPartition, offset)))
    }

    "endOffsets" in new Scope {
      consumer.endOffsets(partitions) should produce(Map((topicPartition, offset)))
    }

    "endOffsets with timeout" in new Scope {
      consumer.endOffsets(partitions, 1.second) should produce(Map((topicPartition, offset)))
    }

    "close" in new Scope {
      verify(consumer.close) { _ =>
        close shouldEqual true
      }
    }

    "close with timeout" in new Scope {
      verify(consumer.close(1.second)) { _ =>
        closeTimeout shouldEqual Some(1.second)
      }
    }

    "wakeup" in new Scope {
      verify(consumer.wakeup) { _ =>
        wakeup shouldEqual true
      }
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
      implicit val ec = CurrentThreadExecutionContext
      implicit val cs = IO.contextShift(ec)
      val metrics = Metrics.empty[IO]
      val consumer = Consumer[IO, Bytes, Bytes](consumerJ, ec)
      Consumer[IO, Bytes, Bytes](consumer, metrics)
    }
  }
}
