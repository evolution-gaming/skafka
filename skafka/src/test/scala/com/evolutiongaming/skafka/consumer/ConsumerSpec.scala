package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.temporal.ChronoUnit
import java.time.{Instant, Duration => DurationJ}
import java.util
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util.{Optional, Collection => CollectionJ, Map => MapJ}

import cats.arrow.FunctionK
import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.effect.IO
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.IOMatchers._
import com.evolutiongaming.skafka.IOSuite._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import org.apache.kafka.clients.consumer.{Consumer => ConsumerJ, ConsumerGroupMetadata => ConsumerGroupMetadataJ, ConsumerRebalanceListener => ConsumerRebalanceListenerJ, ConsumerRecords => ConsumerRecordsJ, OffsetAndMetadata => OffsetAndMetadataJ, OffsetCommitCallback => OffsetCommitCallbackJ}
import org.apache.kafka.common.{Node, TopicPartition => TopicPartitionJ}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

class ConsumerSpec extends AnyWordSpec with Matchers {

  private val topic = "topic"
  private val partition = Partition.min
  private val offset = Offset.min
  private val topicPartition = TopicPartition(topic, partition)
  private val offsetAndMetadata = OffsetAndMetadata(offset, "metadata")
  private val offsets = Nem.of((topicPartition, offsetAndMetadata))
  private val partitions = Nes.of(topicPartition)
  private val instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)
  private val offsetAndTimestamp = OffsetAndTimestamp(offset, instant)
  private val consumerRecord = ConsumerRecord(
    topicPartition = topicPartition,
    offset = offset,
    timestampAndType = Some(TimestampAndType(instant, TimestampType.Create)),
    key = Some(WithSize(Bytes.empty, 1)),
    value = Some(WithSize(Bytes.empty, 1)),
    headers = List(Header("key", Bytes.empty)))
  private val consumerRecords = ConsumerRecords(Map((topicPartition, Nel.of(consumerRecord))))

  private val node = new Node(1, "host", 2)

  private val partitionInfo = PartitionInfo(
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
      verify(consumer.subscribe(Nes.of(topic), Some(rebalanceListener))) { _ =>
        subscribeTopics shouldEqual List(topic)
      }
    }

    "subscribe pattern" in new Scope {
      val pattern = Pattern.compile(".")
      verify(consumer.subscribe(pattern, Some(rebalanceListener))) { _ =>
        subscribePattern shouldEqual Some(pattern)
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
      consumer.commitLater should produce(offsets.toSortedMap.toMap)
    }

    "commitLater offsets" in new Scope {
      verify(consumer.commitLater(offsets)) { _ =>
        commitLater shouldEqual offsets
      }
    }

    "seek" in new Scope {
      verify(consumer.seek(topicPartition, offset)) { _ =>
        seek shouldEqual Some((topicPartition.asJava, offset.value))
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
      consumer.committed(partitions) should produce(offsets.toSortedMap.toMap)
    }

    "committed with timeout" in new Scope {
      consumer.committed(partitions, 1.second) should produce(offsets.toSortedMap.toMap)
    }

    "committed with nulls " in new Scope {
      override def committedNull = true
      consumer.committed(partitions) should produce(Map.empty[TopicPartition, OffsetAndMetadata])
    }

    "committed with timeout with nulls" in new Scope {
      override def committedNull = true
      consumer.committed(partitions, 1.second) should produce(Map.empty[TopicPartition, OffsetAndMetadata])
    }

    "partitions" in new Scope {
      consumer.partitions(topic) should produce(List(partitionInfo))
    }

    "partitions with timeout" in new Scope {
      consumer.partitions(topic, 1.second) should produce(List(partitionInfo))
    }

    "topics" in new Scope {
      consumer.topics should produce(Map((topic, List(partitionInfo))))
    }

    "topics with timeout" in new Scope {
      consumer.topics(1.second) should produce(Map((topic, List(partitionInfo))))
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

    "groupMetadata" in new Scope {
      val consumerGroupMetadata = ConsumerGroupMetadata(
        groupId = "groupId",
        generationId = 123,
        memberId = "memberId",
        groupInstanceId = "groupInstanceId".some)
      consumer.groupMetadata should produce(consumerGroupMetadata)
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

    var pause = List.empty[TopicPartitionJ]

    var resume = List.empty[TopicPartitionJ]

    var wakeup = false

    var seek = Option.empty[(TopicPartitionJ, Long)]

    var seekToBeginning = List.empty[TopicPartitionJ]

    var seekToEnd = List.empty[TopicPartitionJ]

    val rebalanceListener = new RebalanceListener[IO] {

      def onPartitionsAssigned(partitions: Nes[TopicPartition]) = IO.unit

      def onPartitionsRevoked(partitions: Nes[TopicPartition]) = IO.unit
      
      def onPartitionsLost(partitions: Nes[TopicPartition]) = IO.unit
    }

    protected def committedNull = false

    val consumerJ: ConsumerJ[Bytes, Bytes] = new ConsumerJ[Bytes, Bytes] {

      def groupMetadata() = {
        new ConsumerGroupMetadataJ(
          "groupId",
          123,
          "memberId",
          Optional.of("groupInstanceId"))
      }

      def assignment() = Set(topicPartition.asJava).asJava

      def subscription() = Set(topic).asJava

      def subscribe(topics: CollectionJ[String]) = {}

      def subscribe(topics: CollectionJ[String], callback: ConsumerRebalanceListenerJ) = {
        subscribeTopics = topics.asScala.toList
      }

      def assign(partitions: CollectionJ[TopicPartitionJ]) = {
        Scope.this.assign = partitions.asScala.toList
      }

      def subscribe(pattern: Pattern, callback: ConsumerRebalanceListenerJ) = {
        subscribePattern = Some(pattern)
      }

      def subscribe(pattern: Pattern) = {}

      def unsubscribe() = {
        Scope.this.unsubscribe = true
      }

      def poll(timeout: Long) = {
        val records = Map((topicPartition, List(consumerRecord.asJava))).asJavaMap(_.asJava, _.asJava)
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
        Scope.this.commitSync = (offsets.asScalaMap(_.asScala[Try], _.asScala[Try]).get, None).some
      }

      def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], timeout: DurationJ) = {
        Scope.this.commitSync = (offsets.asScalaMap(_.asScala[Try], _.asScala[Try]).get, Some(timeout.asScala)).some
      }

      def commitAsync() = {}

      def commitAsync(callback: OffsetCommitCallbackJ) = {
        callback.onComplete(offsets.toSortedMap.asJavaMap(_.asJava, _.asJava), null)
      }

      def commitAsync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], callback: OffsetCommitCallbackJ) = {
        commitLater = offsets.asScalaMap(_.asScala[Try], _.asScala[Try]).get
        callback.onComplete(offsets, null)
      }

      def seek(partition: TopicPartitionJ, offset: Long) = {
        Scope.this.seek = Some((partition, offset))
      }

      def seek(partition: TopicPartitionJ, offsetAndMetadata: OffsetAndMetadataJ) = {}

      def seekToBeginning(partitions: CollectionJ[TopicPartitionJ]) = {
        Scope.this.seekToBeginning = partitions.asScala.toList
      }

      def seekToEnd(partitions: CollectionJ[TopicPartitionJ]) = {
        Scope.this.seekToEnd = partitions.asScala.toList
      }

      def position(partition: TopicPartitionJ) = offset.value

      def position(partition: TopicPartitionJ, timeout: DurationJ) = position(partition)


      def committed(partition: TopicPartitionJ) = offsetAndMetadata.asJava

      def committed(partition: TopicPartitionJ, timeout: DurationJ) = offsetAndMetadata.asJava

      def committed(partitions: util.Set[TopicPartitionJ]) =
        if (!committedNull)
          offsets.toSortedMap.asJavaMap(_.asJava, _.asJava)
        else
          Map(new TopicPartitionJ(topic, partition.value) -> null).asJavaMap(identity, identity)

      def committed(partitions: util.Set[TopicPartitionJ], timeout: DurationJ) =
        if (!committedNull)
          offsets.toSortedMap.asJavaMap(_.asJava, _.asJava)
        else
          Map(new TopicPartitionJ(topic, partition.value) -> null).asJavaMap(identity, identity)

      def metrics() = new java.util.HashMap()

      def partitionsFor(topic: Topic) = {
        List(partitionInfo.asJava).asJava
      }

      def partitionsFor(topic: Topic, timeout: DurationJ) = {
        partitionsFor(topic)
      }

      def listTopics() = {
        Map((topic, List(partitionInfo.asJava).asJava)).asJavaMap(x => x, x => x)
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
        Map((topicPartition, offset)).asJavaMap(_.asJava, _.value)
      }

      def beginningOffsets(partitions: CollectionJ[TopicPartitionJ], timeout: DurationJ) = {
        beginningOffsets(partitions)
      }

      def endOffsets(partitions: CollectionJ[TopicPartitionJ]) = {
        Map((topicPartition, offset)).asJavaMap(_.asJava, _.value)
      }

      def endOffsets(partitions: CollectionJ[TopicPartitionJ], timeout: DurationJ) = {
        endOffsets(partitions)
      }

      def close() = {}

      def close(timeout: Long, unit: TimeUnit) = {}

      def close(timeout: DurationJ) = {}

      def wakeup() = {
        Scope.this.wakeup = true
      }
    }

    val consumer: Consumer[IO, Bytes, Bytes] = {
      Consumer
        .fromConsumerJ(consumerJ.pure[IO])
        .allocated
        .toTry
        .get
        ._1
        .withMetrics(ConsumerMetrics.empty[IO].mapK(FunctionK.id))
        .mapK(FunctionK.id, FunctionK.id)
        .withLogging(Log.empty)
    }
  }

}