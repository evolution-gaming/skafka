package com.evolutiongaming.skafka.consumer

import java.util.regex.Pattern
import cats.Monad
import cats.data.{NonEmptyMap as Nem, NonEmptySet as Nes}
import cats.implicits.*
import com.evolutiongaming.catshelper.{Log, MeasureDuration}
import com.evolutiongaming.skafka.{ClientMetric, Offset, OffsetAndMetadata, PartitionInfo, Topic, TopicPartition}
import org.apache.kafka.common.Uuid

import scala.concurrent.duration.FiniteDuration

object ConsumerLogging {

  private sealed abstract class WithLogging

  def apply[F[_]: Monad: MeasureDuration, K, V](
    log: Log[F],
    consumer: Consumer[F, K, V]
  ): Consumer[F, K, V] = {

    new WithLogging with Consumer[F, K, V] {

      def assign(partitions: Nes[TopicPartition]): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.assign(partitions)
          d <- d
          _ <- log.debug(s"assign in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}")
        } yield a
      }

      def assignment: F[Set[TopicPartition]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.assignment
          d <- d
          _ <- log.debug(s"assignment in ${d.toMillis}ms, result: ${a.toList.mkString_(", ")}")
        } yield a
      }

      def subscribe(topics: Nes[Topic], listener: RebalanceListener1[F]): F[Unit] = {
        // TODO RebalanceListener1 implement logging - https://github.com/evolution-gaming/skafka/issues/127
        val listenerLogging = listener // .withLogging(log)

        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(topics, listenerLogging)
          d <- d
          _ <- log.debug(s"subscribe in ${d.toMillis}ms, topics: ${topics.mkString_(", ")}, listener1: true")
        } yield a
      }

      def subscribe(topics: Nes[Topic]): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(topics)
          d <- d
          _ <- log.debug(s"subscribe in ${d.toMillis}ms, topics: ${topics.mkString_(", ")}")
        } yield a
      }

      def subscribe(pattern: Pattern, listener: RebalanceListener1[F]): F[Unit] = {
        // TODO RebalanceListener1 implement logging - https://github.com/evolution-gaming/skafka/issues/127
        val listenerLogging = listener // .withLogging(log)

        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(pattern, listenerLogging)
          d <- d
          _ <- log.debug(s"subscribe in ${d.toMillis}ms, pattern: $pattern, listener1: true")
        } yield a
      }

      def subscribe(pattern: Pattern): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(pattern)
          d <- d
          _ <- log.debug(s"subscribe in ${d.toMillis}ms, pattern: $pattern")
        } yield a
      }

      def subscription: F[Set[Topic]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscription
          d <- d
          _ <- log.debug(s"subscription in ${d.toMillis}ms, result: ${a.toList.mkString_(", ")}")
        } yield a
      }

      def unsubscribe: F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.unsubscribe
          d <- d
          _ <- log.debug(s"unsubscribe in ${d.toMillis}ms")
        } yield a
      }

      def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] = {

        def show(records: ConsumerRecords[K, V]) = {
          ConsumerRecords.summaryShow.show(records)
        }

        for {
          d <- MeasureDuration[F].start
          a <- consumer.poll(timeout)
          d <- d
          _ <- log.debug(s"poll timeout: $timeout in ${d.toMillis}ms, result: ${show(a)}")
        } yield a
      }

      def commit: F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit
          d <- d
          _ <- log.debug(s"commit in ${d.toMillis}ms")
        } yield a
      }

      def commit(timeout: FiniteDuration): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit(timeout)
          d <- d
          _ <- log.debug(s"commit in ${d.toMillis}ms, timeout: $timeout")
        } yield a
      }

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit(offsets)
          d <- d
          _ <- log.debug(s"commit in ${d.toMillis}ms, offsets: ${offsets.mkString_(", ")}")
        } yield a
      }

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit(offsets, timeout)
          d <- d
          _ <- log.debug(s"commit in ${d.toMillis}ms, offsets: ${offsets.mkString_(", ")}, timeout: $timeout")
        } yield a
      }

      def commitLater: F[Map[TopicPartition, OffsetAndMetadata]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commitLater
          d <- d
          _ <- log.debug(s"commitLater in ${d.toMillis}ms")
        } yield a
      }

      def commitLater(offsets: Nem[TopicPartition, OffsetAndMetadata]): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commitLater(offsets)
          d <- d
          _ <- log.debug(s"commitLater in ${d.toMillis}ms, offsets: ${offsets.mkString_(", ")}")
        } yield a
      }

      def seek(partition: TopicPartition, offset: Offset): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seek(partition, offset)
          d <- d
          _ <- log.debug(s"seek in ${d.toMillis}ms, partition: $partition, offset: $offset")
        } yield a
      }

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seek(partition, offsetAndMetadata)
          d <- d
          _ <- log.debug(s"seek in ${d.toMillis}ms, partition: $partition, offset: $offsetAndMetadata")
        } yield a
      }

      def seekToBeginning(partitions: Nes[TopicPartition]): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seekToBeginning(partitions)
          d <- d
          _ <- log.debug(s"seekToBeginning in ${d.toMillis}ms, partition: ${partitions.mkString_(", ")}")
        } yield a
      }

      def seekToEnd(partitions: Nes[TopicPartition]): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seekToEnd(partitions)
          d <- d
          _ <- log.debug(s"seekToEnd in ${d.toMillis}ms, partition: ${partitions.mkString_(", ")}")
        } yield a
      }

      def position(partition: TopicPartition): F[Offset] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.position(partition)
          d <- d
          _ <- log.debug(s"position in ${d.toMillis}ms, partition: $partition, result: $a")
        } yield a
      }

      def position(partition: TopicPartition, timeout: FiniteDuration): F[Offset] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.position(partition, timeout)
          d <- d
          _ <- log.debug(s"position in ${d.toMillis}ms, partition: $partition, timeout: $timeout, result: $a")
        } yield a
      }

      def committed(partitions: Nes[TopicPartition]): F[Map[TopicPartition, OffsetAndMetadata]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.committed(partitions)
          d <- d
          _ <- log.debug(s"committed in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}, result: $a")
        } yield a
      }

      def committed(partitions: Nes[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, OffsetAndMetadata]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.committed(partitions, timeout)
          d <- d
          _ <- log.debug(
            s"committed in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}, timeout: $timeout, result: $a"
          )
        } yield a
      }

      def clientInstanceId(timeout: FiniteDuration): F[Uuid] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.clientInstanceId(timeout)
          d <- d
          _ <- log.debug(s"clientInstanceId in ${d.toMillis}ms")
        } yield a
      }

      def partitions(topic: Topic): F[List[PartitionInfo]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.partitions(topic)
          d <- d
          _ <- log.debug(s"partitions in ${d.toMillis}ms, topic: $topic, result: ${a.mkString(", ")}")
        } yield a
      }

      def partitions(topic: Topic, timeout: FiniteDuration): F[List[PartitionInfo]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.partitions(topic, timeout)
          d <- d
          _ <- log.debug(
            s"partitions in ${d.toMillis}ms, topic: $topic, timeout: $timeout, result: ${a.mkString(", ")}"
          )
        } yield a
      }

      def topics: F[Map[Topic, List[PartitionInfo]]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.topics
          d <- d
          _ <- log.debug(s"topics in ${d.toMillis}ms, result: ${a.mkString(", ")}")
        } yield a
      }

      def topics(timeout: FiniteDuration): F[Map[Topic, List[PartitionInfo]]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.topics(timeout)
          d <- d
          _ <- log.debug(s"topics in ${d.toMillis}ms, timeout: $timeout, result: ${a.mkString(", ")}")
        } yield a
      }

      def pause(partitions: Nes[TopicPartition]): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.pause(partitions)
          d <- d
          _ <- log.debug(s"pause in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}")
        } yield a
      }

      def paused: F[Set[TopicPartition]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.paused
          d <- d
          _ <- log.debug(s"paused in ${d.toMillis}ms")
        } yield a
      }

      def resume(partitions: Nes[TopicPartition]): F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.resume(partitions)
          d <- d
          _ <- log.debug(s"resume in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}")
        } yield a
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]): F[Map[TopicPartition, Option[OffsetAndTimestamp]]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.offsetsForTimes(timestampsToSearch)
          d <- d
          _ <- log.debug(
            s"offsetsForTimes in ${d.toMillis}ms, partitions: ${timestampsToSearch.mkString(", ")}, result: ${a.mkString(", ")}"
          )
        } yield a
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration): F[Map[TopicPartition, Option[OffsetAndTimestamp]]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.offsetsForTimes(timestampsToSearch, timeout)
          d <- d
          _ <- log.debug(s"offsetsForTimes in ${d.toMillis}ms, partitions: ${timestampsToSearch
              .mkString(", ")}, timeout: $timeout, result: ${a.mkString(", ")}")
        } yield a
      }

      def beginningOffsets(partitions: Nes[TopicPartition]): F[Map[TopicPartition, Offset]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.beginningOffsets(partitions)
          d <- d
          _ <- log.debug(
            s"beginningOffsets in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}, result: ${a.mkString(", ")}"
          )
        } yield a
      }

      def beginningOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, Offset]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.beginningOffsets(partitions, timeout)
          d <- d
          _ <- log.debug(s"beginningOffsets in ${d.toMillis}ms, partitions: ${partitions
              .mkString_(", ")}, timeout: $timeout, result: ${a.mkString(", ")}")
        } yield a
      }

      def endOffsets(partitions: Nes[TopicPartition]): F[Map[TopicPartition, Offset]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.endOffsets(partitions)
          d <- d
          _ <- log.debug(
            s"endOffsets in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}, result: ${a.mkString(", ")}"
          )
        } yield a
      }

      def endOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, Offset]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.endOffsets(partitions, timeout)
          d <- d
          _ <- log.debug(
            s"endOffsets in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}, timeout: $timeout, result: ${a.mkString(", ")}"
          )
        } yield a
      }

      def currentLag(partition: TopicPartition): F[Option[Long]] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.currentLag(partition)
          d <- d
          _ <- log.debug(s"currentLag in ${d.toMillis}ms, partition: $partition, result: $a")
        } yield a
      }

      def groupMetadata: F[ConsumerGroupMetadata] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.groupMetadata
          d <- d
          _ <- log.debug(s"groupMetadata in ${d.toMillis}ms, result: $a")
        } yield a
      }

      def wakeup: F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.wakeup
          d <- d
          _ <- log.debug(s"wakeup in ${d.toMillis}ms")
        } yield a
      }

      def enforceRebalance: F[Unit] = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.enforceRebalance
          d <- d
          _ <- log.debug(s"enforceRebalance in ${d.toMillis}ms")
        } yield a
      }

      def clientMetrics: F[Seq[ClientMetric[F]]] = consumer.clientMetrics
    }
  }
}
