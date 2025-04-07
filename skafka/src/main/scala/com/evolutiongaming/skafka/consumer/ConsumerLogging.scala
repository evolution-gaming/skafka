package com.evolutiongaming.skafka.consumer

import java.util.regex.Pattern
import cats.Monad
import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.implicits._
import com.evolutiongaming.catshelper.{Log, MeasureDuration}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Topic, TopicPartition}

import scala.annotation.nowarn
import scala.concurrent.duration.FiniteDuration

object ConsumerLogging {

  private sealed abstract class WithLogging

  def apply[F[_]: Monad: MeasureDuration, K, V](
    log: Log[F],
    consumer: Consumer[F, K, V]
  ): Consumer[F, K, V] = {

    new WithLogging with Consumer[F, K, V] {

      def assign(partitions: Nes[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.assign(partitions)
          d <- d
          _ <- log.debug(s"assign in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}")
        } yield a
      }

      def assignment = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.assignment
          d <- d
          _ <- log.debug(s"assignment in ${d.toMillis}ms, result: ${a.toList.mkString_(", ")}")
        } yield a
      }

      def subscribe(topics: Nes[Topic], listener: RebalanceListener1[F]) = {
        // TODO RebalanceListener1 implement logging - https://github.com/evolution-gaming/skafka/issues/127
        val listenerLogging = listener //.withLogging(log)

        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(topics, listenerLogging)
          d <- d
          _ <- log.debug(s"subscribe in ${d.toMillis}ms, topics: ${topics.mkString_(", ")}, listener1: true")
        } yield a
      }

      def subscribe(topics: Nes[Topic]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(topics)
          d <- d
          _ <- log.debug(s"subscribe in ${d.toMillis}ms, topics: ${topics.mkString_(", ")}")
        } yield a
      }

      def subscribe(pattern: Pattern, listener: RebalanceListener1[F]) = {
        // TODO RebalanceListener1 implement logging - https://github.com/evolution-gaming/skafka/issues/127
        val listenerLogging = listener //.withLogging(log)

        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(pattern, listenerLogging)
          d <- d
          _ <- log.debug(s"subscribe in ${d.toMillis}ms, pattern: $pattern, listener1: true")
        } yield a
      }

      def subscribe(pattern: Pattern) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(pattern)
          d <- d
          _ <- log.debug(s"subscribe in ${d.toMillis}ms, pattern: $pattern")
        } yield a
      }

      @nowarn("cat=deprecation")
      def subscribe(topics: Nes[Topic], listener: Option[RebalanceListener[F]]) = {

        val listenerLogging = (listener getOrElse RebalanceListener.empty[F]).withLogging(log)

        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(topics, listenerLogging.some)
          d <- d
          _ <- log.debug(
            s"subscribe in ${d.toMillis}ms, topics: ${topics.mkString_(", ")}, listener: ${listener.isDefined}"
          )
        } yield a
      }

      @nowarn("cat=deprecation")
      def subscribe(pattern: Pattern, listener: Option[RebalanceListener[F]]) = {

        val listenerLogging = (listener getOrElse RebalanceListener.empty[F]).withLogging(log)

        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(pattern, listenerLogging.some)
          d <- d
          _ <- log.debug(s"subscribe in ${d.toMillis}ms, pattern: $pattern, listener: ${listener.isDefined}")
        } yield a
      }

      def subscription = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscription
          d <- d
          _ <- log.debug(s"subscription in ${d.toMillis}ms, result: ${a.toList.mkString_(", ")}")
        } yield a
      }

      def unsubscribe = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.unsubscribe
          d <- d
          _ <- log.debug(s"unsubscribe in ${d.toMillis}ms")
        } yield a
      }

      def poll(timeout: FiniteDuration) = {

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

      def commit = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit
          d <- d
          _ <- log.debug(s"commit in ${d.toMillis}ms")
        } yield a
      }

      def commit(timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit(timeout)
          d <- d
          _ <- log.debug(s"commit in ${d.toMillis}ms, timeout: $timeout")
        } yield a
      }

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit(offsets)
          d <- d
          _ <- log.debug(s"commit in ${d.toMillis}ms, offsets: ${offsets.mkString_(", ")}")
        } yield a
      }

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit(offsets, timeout)
          d <- d
          _ <- log.debug(s"commit in ${d.toMillis}ms, offsets: ${offsets.mkString_(", ")}, timeout: $timeout")
        } yield a
      }

      def commitLater = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commitLater
          d <- d
          _ <- log.debug(s"commitLater in ${d.toMillis}ms")
        } yield a
      }

      def commitLater(offsets: Nem[TopicPartition, OffsetAndMetadata]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commitLater(offsets)
          d <- d
          _ <- log.debug(s"commitLater in ${d.toMillis}ms, offsets: ${offsets.mkString_(", ")}")
        } yield a
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seek(partition, offset)
          d <- d
          _ <- log.debug(s"seek in ${d.toMillis}ms, partition: $partition, offset: $offset")
        } yield a
      }

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seek(partition, offsetAndMetadata)
          d <- d
          _ <- log.debug(s"seek in ${d.toMillis}ms, partition: $partition, offset: $offsetAndMetadata")
        } yield a
      }

      def seekToBeginning(partitions: Nes[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seekToBeginning(partitions)
          d <- d
          _ <- log.debug(s"seekToBeginning in ${d.toMillis}ms, partition: ${partitions.mkString_(", ")}")
        } yield a
      }

      def seekToEnd(partitions: Nes[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seekToEnd(partitions)
          d <- d
          _ <- log.debug(s"seekToEnd in ${d.toMillis}ms, partition: ${partitions.mkString_(", ")}")
        } yield a
      }

      def position(partition: TopicPartition) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.position(partition)
          d <- d
          _ <- log.debug(s"position in ${d.toMillis}ms, partition: $partition, result: $a")
        } yield a
      }

      def position(partition: TopicPartition, timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.position(partition, timeout)
          d <- d
          _ <- log.debug(s"position in ${d.toMillis}ms, partition: $partition, timeout: $timeout, result: $a")
        } yield a
      }

      def committed(partitions: Nes[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.committed(partitions)
          d <- d
          _ <- log.debug(s"committed in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}, result: $a")
        } yield a
      }

      def committed(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.committed(partitions, timeout)
          d <- d
          _ <- log.debug(
            s"committed in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}, timeout: $timeout, result: $a"
          )
        } yield a
      }

      def partitions(topic: Topic) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.partitions(topic)
          d <- d
          _ <- log.debug(s"partitions in ${d.toMillis}ms, topic: $topic, result: ${a.mkString(", ")}")
        } yield a
      }

      def partitions(topic: Topic, timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.partitions(topic, timeout)
          d <- d
          _ <- log.debug(
            s"partitions in ${d.toMillis}ms, topic: $topic, timeout: $timeout, result: ${a.mkString(", ")}"
          )
        } yield a
      }

      def topics = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.topics
          d <- d
          _ <- log.debug(s"topics in ${d.toMillis}ms, result: ${a.mkString(", ")}")
        } yield a
      }

      def topics(timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.topics(timeout)
          d <- d
          _ <- log.debug(s"topics in ${d.toMillis}ms, timeout: $timeout, result: ${a.mkString(", ")}")
        } yield a
      }

      def pause(partitions: Nes[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.pause(partitions)
          d <- d
          _ <- log.debug(s"pause in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}")
        } yield a
      }

      def paused = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.paused
          d <- d
          _ <- log.debug(s"paused in ${d.toMillis}ms")
        } yield a
      }

      def resume(partitions: Nes[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.resume(partitions)
          d <- d
          _ <- log.debug(s"resume in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}")
        } yield a
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.offsetsForTimes(timestampsToSearch)
          d <- d
          _ <- log.debug(
            s"offsetsForTimes in ${d.toMillis}ms, partitions: ${timestampsToSearch.mkString(", ")}, result: ${a.mkString(", ")}"
          )
        } yield a
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.offsetsForTimes(timestampsToSearch, timeout)
          d <- d
          _ <- log.debug(s"offsetsForTimes in ${d.toMillis}ms, partitions: ${timestampsToSearch
            .mkString(", ")}, timeout: $timeout, result: ${a.mkString(", ")}")
        } yield a
      }

      def beginningOffsets(partitions: Nes[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.beginningOffsets(partitions)
          d <- d
          _ <- log.debug(
            s"beginningOffsets in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}, result: ${a.mkString(", ")}"
          )
        } yield a
      }

      def beginningOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.beginningOffsets(partitions, timeout)
          d <- d
          _ <- log.debug(s"beginningOffsets in ${d.toMillis}ms, partitions: ${partitions
            .mkString_(", ")}, timeout: $timeout, result: ${a.mkString(", ")}")
        } yield a
      }

      def endOffsets(partitions: Nes[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.endOffsets(partitions)
          d <- d
          _ <- log.debug(
            s"endOffsets in ${d.toMillis}ms, partitions: ${partitions.mkString_(", ")}, result: ${a.mkString(", ")}"
          )
        } yield a
      }

      def endOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
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

      def groupMetadata = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.groupMetadata
          d <- d
          _ <- log.debug(s"groupMetadata in ${d.toMillis}ms, result: $a")
        } yield a
      }

      def wakeup = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.wakeup
          d <- d
          _ <- log.debug(s"wakeup in ${d.toMillis}ms")
        } yield a
      }

      def enforceRebalance = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.enforceRebalance
          d <- d
          _ <- log.debug(s"enforceRebalance in ${d.toMillis}ms")
        } yield a
      }

      def clientMetrics = consumer.clientMetrics
    }
  }
}
