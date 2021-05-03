package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.Instant
import java.util.{Map => MapJ}

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import com.evolutiongaming.skafka._
import org.apache.kafka.clients.consumer.{Consumer => ConsumerJ, OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Internal wrapper for [[org.apache.kafka.clients.consumer.Consumer]]
  * with a smaller scope of methods making sense during consumer group rebalance.
  * Introduced in https://github.com/evolution-gaming/skafka/pull/122
  * At the moment of writing we had KafkaConsumer v2.5.1
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
trait RebalanceConsumer {

  def assignment(): Try[Set[TopicPartition]]

  def beginningOffsets(partitions: Nes[TopicPartition]): Try[Map[TopicPartition, Offset]]

  def beginningOffsets(
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): Try[Map[TopicPartition, Offset]]

  def commit(): Try[Unit]

  def commit(timeout: FiniteDuration): Try[Unit]

  def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]): Try[Unit]

  def commit(offsets: Nem[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): Try[Unit]

  def committed(partitions: Nes[TopicPartition]): Try[Map[TopicPartition, OffsetAndMetadata]]

  def committed(partitions: Nes[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, OffsetAndMetadata]]

  def endOffsets(partitions: Nes[TopicPartition]): Try[Map[TopicPartition, Offset]]

  def endOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, Offset]]

  def groupMetadata(): Try[ConsumerGroupMetadata]

  def topics(): Try[Map[Topic, List[PartitionInfo]]]

  def topics(timeout: FiniteDuration): Try[Map[Topic, List[PartitionInfo]]]

  def offsetsForTimes(
    timestampsToSearch: Nem[TopicPartition, Instant]
  ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]]

  def offsetsForTimes(
    timestampsToSearch: Nem[TopicPartition, Instant],
    timeout: FiniteDuration
  ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]]

  def partitionsFor(topic: Topic): Try[List[PartitionInfo]]

  def partitionsFor(topic: Topic, timeout: FiniteDuration): Try[List[PartitionInfo]]

  def paused(): Try[Set[TopicPartition]]

  def position(partition: TopicPartition): Try[Offset]

  def position(partition: TopicPartition, timeout: FiniteDuration): Try[Offset]

  def seek(partition: TopicPartition, offset: Offset): Try[Unit]

  def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Try[Unit]

  def seekToBeginning(partitions: Nes[TopicPartition]): Try[Unit]

  def seekToEnd(partitions: Nes[TopicPartition]): Try[Unit]

  def subscription(): Try[Set[Topic]]

}

object RebalanceConsumer {
  def apply(c: ConsumerJ[_, _]): RebalanceConsumer = new RebalanceConsumer {
    def assignment(): Try[Set[TopicPartition]] =
      for {
        a <- Try { c.assignment() }
        a <- topicPartitionsSetF[Try](a)
      } yield a

    def beginningOffsets(partitions: Nes[TopicPartition]): Try[Map[TopicPartition, Offset]] =
      offsets1(_.beginningOffsets(partitions.asJava))

    def beginningOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, Offset]] =
      offsets1(_.beginningOffsets(partitions.asJava, timeout.asJava))

    def commit(): Try[Unit] =
      Try { c.commitSync() }

    def commit(timeout: FiniteDuration): Try[Unit] =
      Try { c.commitSync(timeout.asJava) }

    def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]): Try[Unit] =
      Try { c.commitSync(asOffsetsAndMetadataJ(offsets)) }

    def commit(offsets: Nem[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): Try[Unit] =
      Try { c.commitSync(asOffsetsAndMetadataJ(offsets), timeout.asJava) }

    def committed(partitions: Nes[TopicPartition]): Try[Map[TopicPartition, OffsetAndMetadata]] =
      committed1(_.committed(partitions.asJava))

    def committed(
      partitions: Nes[TopicPartition],
      timeout: FiniteDuration
    ): Try[Map[TopicPartition, OffsetAndMetadata]] =
      committed1(_.committed(partitions.asJava, timeout.asJava))

    def endOffsets(partitions: Nes[TopicPartition]): Try[Map[TopicPartition, Offset]] =
      offsets1(_.endOffsets(partitions.asJava))

    def endOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration): Try[Map[TopicPartition, Offset]] =
      offsets1(_.endOffsets(partitions.asJava, timeout.asJava))

    def groupMetadata(): Try[ConsumerGroupMetadata] = Try { c.groupMetadata().asScala }

    def topics(): Try[Map[Topic, List[PartitionInfo]]] =
      for {
        a <- Try { c.listTopics() }
        a <- partitionsInfoMapF[Try](a)
      } yield a

    def topics(timeout: FiniteDuration): Try[Map[Topic, List[PartitionInfo]]] =
      for {
        a <- Try { c.listTopics(timeout.asJava) }
        a <- partitionsInfoMapF[Try](a)
      } yield a

    def offsetsForTimes(
      timestampsToSearch: Nem[TopicPartition, Instant]
    ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
      for {
        a <- Try { c.offsetsForTimes(timestampsToSearchJ(timestampsToSearch)) }
        a <- offsetsAndTimestampsMapF[Try](a)
      } yield a

    def offsetsForTimes(
      timestampsToSearch: Nem[TopicPartition, Instant],
      timeout: FiniteDuration
    ): Try[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
      for {
        a <- Try { c.offsetsForTimes(timestampsToSearchJ(timestampsToSearch), timeout.asJava) }
        a <- offsetsAndTimestampsMapF[Try](a)
      } yield a

    def partitionsFor(topic: Topic): Try[List[PartitionInfo]] =
      for {
        a <- Try { c.partitionsFor(topic) }
        a <- partitionsInfoListF[Try](a)
      } yield a

    def partitionsFor(topic: Topic, timeout: FiniteDuration): Try[List[PartitionInfo]] =
      for {
        a <- Try { c.partitionsFor(topic, timeout.asJava) }
        a <- partitionsInfoListF[Try](a)
      } yield a

    def paused(): Try[Set[TopicPartition]] =
      for {
        a <- Try { c.paused() }
        a <- topicPartitionsSetF[Try](a)
      } yield a

    def position(partition: TopicPartition): Try[Offset] =
      for {
        a <- Try { c.position(partition.asJava) }
        a <- Offset.of[Try](a)
      } yield a

    def position(partition: TopicPartition, timeout: FiniteDuration): Try[Offset] =
      for {
        a <- Try { c.position(partition.asJava, timeout.asJava) }
        a <- Offset.of[Try](a)
      } yield a

    def seek(partition: TopicPartition, offset: Offset): Try[Unit] =
      Try { c.seek(partition.asJava, offset.value) }

    def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Try[Unit] =
      Try { c.seek(partition.asJava, offsetAndMetadata.asJava) }

    def seekToBeginning(partitions: Nes[TopicPartition]): Try[Unit] =
      Try { c.seekToBeginning(partitions.asJava) }

    def seekToEnd(partitions: Nes[TopicPartition]): Try[Unit] =
      Try { c.seekToEnd(partitions.asJava) }

    def subscription(): Try[Set[Topic]] =
      Try { c.subscription().asScala.toSet }

    private def committed1(
      f: ConsumerJ[_, _] => MapJ[TopicPartitionJ, OffsetAndMetadataJ]
    ): Try[Map[TopicPartition, OffsetAndMetadata]] = {
      for {
        a <- Try { f(c) }
        a <- committedOffsetsF[Try](a)
      } yield a
    }

    private def offsets1(
      f: ConsumerJ[_, _] => MapJ[TopicPartitionJ, LongJ]
    ): Try[Map[TopicPartition, Offset]] = {
      for {
        a <- Try(f(c))
        a <- offsetsMapF[Try](a)
      } yield a
    }
  }
}
