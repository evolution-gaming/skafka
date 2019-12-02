package com.evolutiongaming.skafka.consumer

import java.util.regex.Pattern

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Topic, TopicPartition}
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.duration.FiniteDuration

object ConsumerLogging {

  def apply[F[_] : Monad : MeasureDuration, K, V](
    log: Log[F],
    consumer: Consumer[F, K, V]
  ): Consumer[F, K, V] = {

    new Consumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.assign(partitions)
          d <- d
          _ <- log.debug(s"assign partitions: ${ partitions.mkString_(", ") } in ${ d.toMillis }ms")
        } yield a
      }

      val assignment = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.assignment
          d <- d
          _ <- log.debug(s"assignment in ${ d.toMillis }ms, result: ${ a.toList.mkString_(", ") }")
        } yield a
      }

      def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener[F]]) = {

        val listenerLogging = (listener getOrElse RebalanceListener.empty[F]).withLogging(log)

        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(topics, listenerLogging.some)
          d <- d
          _ <- log.debug(s"subscribe topics: ${ topics.mkString_(", ") }, listener: ${ listener.isDefined } in ${ d.toMillis }ms")
        } yield a
      }

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener[F]]) = {

        val listenerLogging = (listener getOrElse RebalanceListener.empty[F]).withLogging(log)

        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscribe(pattern, listenerLogging.some)
          d <- d
          _ <- log.debug(s"subscribe pattern: $pattern, listener: ${ listener.isDefined } in ${ d.toMillis }ms")
        } yield a
      }

      val subscription = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.subscription
          d <- d
          _ <- log.debug(s"subscription in ${ d.toMillis }ms, result: ${ a.toList.mkString_(", ") }")
        } yield a
      }

      val unsubscribe = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.unsubscribe
          d <- d
          _ <- log.debug(s"unsubscribe in ${ d.toMillis }ms")
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
          _ <- log.debug(s"poll timeout: $timeout in ${ d.toMillis }ms, result: ${ show(a) }")
        } yield a
      }

      val commit = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit
          d <- d
          _ <- log.debug(s"commit in ${ d.toMillis }ms")
        } yield a
      }

      def commit(timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit(timeout)
          d <- d
          _ <- log.debug(s"commit timeout: $timeout in ${ d.toMillis }ms")
        } yield a
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit(offsets)
          d <- d
          _ <- log.debug(s"commit offsets: ${ offsets.mkString(", ") } in ${ d.toMillis }ms")
        } yield a
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commit(offsets, timeout)
          d <- d
          _ <- log.debug(s"commit offsets: ${ offsets.mkString(", ") }, timeout: $timeout in ${ d.toMillis }ms")
        } yield a
      }

      val commitLater = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commitLater
          d <- d
          _ <- log.debug(s"commitLater in ${ d.toMillis }ms")
        } yield a
      }

      def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.commitLater(offsets)
          d <- d
          _ <- log.debug(s"commitLater offsets: ${ offsets.mkString(", ") } in ${ d.toMillis }ms")
        } yield a
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seek(partition, offset)
          d <- d
          _ <- log.debug(s"seek partition: $partition, offset: $offset in ${ d.toMillis }ms")
        } yield a
      }

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seek(partition, offsetAndMetadata)
          d <- d
          _ <- log.debug(s"seek partition: $partition, offset: $offsetAndMetadata in ${ d.toMillis }ms")
        } yield a
      }

      def seekToBeginning(partitions: Nel[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seekToBeginning(partitions)
          d <- d
          _ <- log.debug(s"seekToBeginning partition: ${ partitions.mkString_(", ") } in ${ d.toMillis }ms")
        } yield a
      }

      def seekToEnd(partitions: Nel[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.seekToEnd(partitions)
          d <- d
          _ <- log.debug(s"seekToEnd partition: ${ partitions.mkString_(", ") } in ${ d.toMillis }ms")
        } yield a
      }

      def position(partition: TopicPartition) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.position(partition)
          d <- d
          _ <- log.debug(s"position partition: $partition in ${ d.toMillis }ms, result: $a")
        } yield a
      }

      def position(partition: TopicPartition, timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.position(partition, timeout)
          d <- d
          _ <- log.debug(s"position partition: $partition, timeout: $timeout in ${ d.toMillis }ms, result: $a")
        } yield a
      }

      def committed(partition: TopicPartition) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.committed(partition)
          d <- d
          _ <- log.debug(s"committed partition: $partition in ${ d.toMillis }ms, result: $a")
        } yield a
      }

      def committed(partition: TopicPartition, timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.committed(partition, timeout)
          d <- d
          _ <- log.debug(s"committed partition: $partition, timeout: $timeout in ${ d.toMillis }ms, result: $a")
        } yield a
      }

      def partitions(topic: Topic) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.partitions(topic)
          d <- d
          _ <- log.debug(s"partitions topic: $topic in ${ d.toMillis }ms, result: ${ a.mkString(", ") }")
        } yield a
      }

      def partitions(topic: Topic, timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.partitions(topic, timeout)
          d <- d
          _ <- log.debug(s"partitions topic: $topic, timeout: $timeout in ${ d.toMillis }ms, result: ${ a.mkString(", ") }")
        } yield a
      }

      val topics = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.topics
          d <- d
          _ <- log.debug(s"topics in ${ d.toMillis }ms, result: ${ a.mkString(", ") }")
        } yield a
      }

      def topics(timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.topics(timeout)
          d <- d
          _ <- log.debug(s"topics timeout: $timeout in ${ d.toMillis }ms, result: ${ a.mkString(", ") }")
        } yield a
      }

      def pause(partitions: Nel[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.pause(partitions)
          d <- d
          _ <- log.debug(s"pause partitions: ${ partitions.mkString_(", ") } in ${ d.toMillis }ms")
        } yield a
      }

      val paused = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.paused
          d <- d
          _ <- log.debug(s"paused in ${ d.toMillis }ms")
        } yield a
      }

      def resume(partitions: Nel[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.resume(partitions)
          d <- d
          _ <- log.debug(s"resume partitions: ${ partitions.mkString_(", ") } in ${ d.toMillis }ms")
        } yield a
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.offsetsForTimes(timestampsToSearch)
          d <- d
          _ <- log.debug(s"offsetsForTimes partitions: ${ timestampsToSearch.mkString(", ") } in ${ d.toMillis }ms, result: ${ a.mkString(", ") }")
        } yield a
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.offsetsForTimes(timestampsToSearch, timeout)
          d <- d
          _ <- log.debug(s"offsetsForTimes partitions: ${ timestampsToSearch.mkString(", ") }, timeout: $timeout in ${ d.toMillis }ms, result: ${ a.mkString(", ") }")
        } yield a
      }

      def beginningOffsets(partitions: Nel[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.beginningOffsets(partitions)
          d <- d
          _ <- log.debug(s"beginningOffsets partitions: ${ partitions.mkString_(", ") } in ${ d.toMillis }ms, result: ${ a.mkString(", ") }")
        } yield a
      }

      def beginningOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.beginningOffsets(partitions, timeout)
          d <- d
          _ <- log.debug(s"beginningOffsets partitions: ${ partitions.mkString_(", ") }, timeout: $timeout in ${ d.toMillis }ms, result: ${ a.mkString(", ") }")
        } yield a
      }

      def endOffsets(partitions: Nel[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.endOffsets(partitions)
          d <- d
          _ <- log.debug(s"endOffsets partitions: ${ partitions.mkString_(", ") } in ${ d.toMillis }ms, result: ${ a.mkString(", ") }")
        } yield a
      }

      def endOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.endOffsets(partitions, timeout)
          d <- d
          _ <- log.debug(s"endOffsets partitions: ${ partitions.mkString_(", ") }, timeout: $timeout in ${ d.toMillis }ms, result: ${ a.mkString(", ") }")
        } yield a
      }

      val wakeup = {
        for {
          d <- MeasureDuration[F].start
          a <- consumer.wakeup
          d <- d
          _ <- log.debug(s"wakeup in ${ d.toMillis }ms")
        } yield a
      }
    }
  }
}
