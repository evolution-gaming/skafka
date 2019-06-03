package com.evolutiongaming.skafka
package consumer

import java.lang.{Long => LongJ}
import java.util.regex.Pattern

import cats.effect.{Sync, ContextShift}
import cats.implicits._
import cats.instances.list.catsStdInstancesForList
import cats.{Applicative, Traverse}
import com.evolutiongaming.catshelper.FromFuture
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetCommitCallback, Consumer => ConsumerJ}

import scala.collection.JavaConverters._
import scala.collection.immutable.Iterable
import scala.compat.Platform
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.Try

/**
  * See [[org.apache.kafka.clients.consumer.Consumer]]
  */
trait Consumer[F[_], K, V] {

  def assign(partitions: Nel[TopicPartition]): F[Unit]

  def assignment: F[Set[TopicPartition]]


  def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]): F[Unit]

  def subscribe(pattern: Pattern, listener: Option[RebalanceListener]): F[Unit]

  def subscription: F[Set[Topic]]

  def unsubscribe: F[Unit]


  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]]


  def commit: F[Unit]

  def commit(timeout: FiniteDuration): F[Unit]

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): F[Unit]


  def commitLater: F[Map[TopicPartition, OffsetAndMetadata]]

  def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]


  def seek(partition: TopicPartition, offset: Offset): F[Unit]

  def seekToBeginning(partitions: Nel[TopicPartition]): F[Unit]

  def seekToEnd(partitions: Nel[TopicPartition]): F[Unit]


  def position(partition: TopicPartition): F[Offset]

  def position(partition: TopicPartition, timeout: FiniteDuration): F[Offset]


  def committed(partition: TopicPartition): F[OffsetAndMetadata]

  def committed(partition: TopicPartition, timeout: FiniteDuration): F[OffsetAndMetadata]


  def partitions(topic: Topic): F[List[PartitionInfo]]

  def partitions(topic: Topic, timeout: FiniteDuration): F[List[PartitionInfo]]


  def listTopics: F[Map[Topic, List[PartitionInfo]]]

  def listTopics(timeout: FiniteDuration): F[Map[Topic, List[PartitionInfo]]]


  def pause(partitions: Nel[TopicPartition]): F[Unit]

  def paused: F[Set[TopicPartition]]

  def resume(partitions: Nel[TopicPartition]): F[Unit]


  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]): F[Map[TopicPartition, Option[OffsetAndTimestamp]]]

  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long], timeout: FiniteDuration): F[Map[TopicPartition, Option[OffsetAndTimestamp]]]


  def beginningOffsets(partitions: Nel[TopicPartition]): F[Map[TopicPartition, Offset]]

  def beginningOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, Offset]]


  def endOffsets(partitions: Nel[TopicPartition]): F[Map[TopicPartition, Offset]]

  def endOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, Offset]]


  def close: F[Unit]

  def close(timeout: FiniteDuration): F[Unit]


  def wakeup: F[Unit]
}


object Consumer {

  def empty[F[_] : Applicative, K, V]: Consumer[F, K, V] = new Consumer[F, K, V] {

    private val empty = ().pure[F]

    override def assign(partitions: Nel[TopicPartition]): F[Unit] = empty

    override val assignment: F[Set[TopicPartition]] = Set.empty[TopicPartition].pure[F]

    override def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]): F[Unit] = empty

    override def subscribe(pattern: Pattern, listener: Option[RebalanceListener]): F[Unit] = empty

    override val subscription: F[Set[Topic]] = Set.empty[Topic].pure[F]
    override val unsubscribe: F[Unit] = empty

    override def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] = ConsumerRecords.empty[K, V].pure[F]

    override val commit: F[Unit] = empty

    override def commit(timeout: FiniteDuration): F[Unit] = empty

    override def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] = empty

    override def commit(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): F[Unit] = empty

    override val commitLater: F[Map[TopicPartition, OffsetAndMetadata]] =
      Map.empty[TopicPartition, OffsetAndMetadata].pure[F]

    override def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] = empty

    override def seek(partition: TopicPartition, offset: Offset): F[Unit] = empty

    override def seekToBeginning(partitions: Nel[TopicPartition]): F[Unit] = empty

    override def seekToEnd(partitions: Nel[TopicPartition]): F[Unit] = empty

    override def position(partition: TopicPartition): F[Offset] = Offset.Min.pure[F]

    override def position(partition: TopicPartition, timeout: FiniteDuration): F[Offset] = Offset.Min.pure[F]

    override def committed(partition: TopicPartition): F[OffsetAndMetadata] = OffsetAndMetadata.empty.pure[F]


    override def committed(partition: TopicPartition, timeout: FiniteDuration): F[OffsetAndMetadata] =
      OffsetAndMetadata.empty.pure[F]

    override def partitions(topic: Topic): F[List[PartitionInfo]] = List.empty[PartitionInfo].pure[F]

    override def partitions(topic: Topic, timeout: FiniteDuration): F[List[PartitionInfo]] =
      List.empty[PartitionInfo].pure[F]

    override val listTopics: F[Map[Topic, List[PartitionInfo]]] =
      Map.empty[Topic, List[PartitionInfo]].pure[F]

    override def listTopics(timeout: FiniteDuration): F[Map[Topic, List[PartitionInfo]]] =
      Map.empty[Topic, List[PartitionInfo]].pure[F]

    override def pause(partitions: Nel[TopicPartition]): F[Unit] = empty

    override val paused: F[Set[TopicPartition]] = Set.empty[TopicPartition].pure[F]

    override def resume(partitions: Nel[TopicPartition]): F[Unit] = empty

    override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) =
      Map.empty[TopicPartition, Option[OffsetAndTimestamp]].pure[F]

    override def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Offset],
      timeout: FiniteDuration): F[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
      Map.empty[TopicPartition, Option[OffsetAndTimestamp]].pure[F]

    override def beginningOffsets(partitions: Nel[TopicPartition]) = Map.empty[TopicPartition, Offset].pure[F]

    override def beginningOffsets(
      partitions: Nel[TopicPartition],
      timeout: FiniteDuration): F[Map[TopicPartition, Offset]] = Map.empty[TopicPartition, Offset].pure[F]

    override def endOffsets(partitions: Nel[TopicPartition]) = Map.empty[TopicPartition, Offset].pure[F]

    override def endOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) =
      Map.empty[TopicPartition, Offset].pure[F]

    override val close: F[Unit] = empty

    override def close(timeout: FiniteDuration): F[Unit] = empty

    override val wakeup: F[Unit] = empty
  }


  def apply[F[_] : Sync : ContextShift : FromFuture, K, V](
    config: ConsumerConfig,
    ecBlocking: ExecutionContext)(
    implicit valueFromBytes: FromBytes[V],
    keyFromBytes: FromBytes[K]): Consumer[F, K, V] = {

    val valueDeserializer = valueFromBytes.asJava
    val keyDeserializer = keyFromBytes.asJava
    val consumerK = new KafkaConsumer(config.properties, keyDeserializer, valueDeserializer)
    apply(consumerK, ecBlocking)
  }

  def apply[F[_] : Sync : ContextShift : FromFuture, K, V](consumer: ConsumerJ[K, V], ecBlocking: ExecutionContext): Consumer[F, K, V] = {
    val blocking = Blocking(ecBlocking)

    def commitWithCallback(f: OffsetCommitCallback => Unit): F[Map[TopicPartition, OffsetAndMetadata]] = {
      val promise = Promise[Map[TopicPartition, OffsetAndMetadata]]()
      val callback = new CommitCallback {
        def apply(offsets: Try[Map[TopicPartition, OffsetAndMetadata]]) = {
          promise.complete(offsets)
        }
      }
      f(callback.asJava)
      blocking.future(promise.future)
    }

    new Consumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = blocking {
        val partitionsJ = partitions.toList.map(_.asJava).asJavaCollection
        consumer.assign(partitionsJ)
      }

      val assignment = blocking {
        val partitionsJ = consumer.assignment()
        partitionsJ.asScala.map(_.asScala).toSet
      }

      def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]) = blocking {
        val topicsJ = topics.asJava
        consumer.subscribe(topicsJ, (listener getOrElse RebalanceListener.empty).asJava)
      }

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener]) = blocking {
        consumer.subscribe(pattern, (listener getOrElse RebalanceListener.empty).asJava)
      }

      val subscription = blocking {
        consumer.subscription().asScala.toSet
      }

      val unsubscribe = blocking {
        consumer.unsubscribe()
      }

      def poll(timeout: FiniteDuration) = blocking {
        val records = consumer.poll(timeout.asJava)
        records.asScala
      }

      val commit = blocking {
        consumer.commitSync()
      }

      def commit(timeout: FiniteDuration) = blocking {
        consumer.commitSync(timeout.asJava)
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) =
        blocking {
          consumer.commitSync(offsets.asJavaMap(_.asJava, _.asJava))
        }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) =
        blocking {
          consumer.commitSync(offsets.asJavaMap(_.asJava, _.asJava), timeout.asJava)
        }

      val commitLater = commitWithCallback(consumer.commitAsync)

      def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]) =
        commitWithCallback(consumer.commitAsync(offsets.deepAsJava, _)) *> Sync[F].unit

      def seek(partition: TopicPartition, offset: Offset) = blocking {
        consumer.seek(partition.asJava, offset)
      }

      def seekToBeginning(partitions: Nel[TopicPartition]) = blocking {
        val partitionsJ = partitions.map(_.asJava).asJava
        consumer.seekToBeginning(partitionsJ)
      }

      def seekToEnd(partitions: Nel[TopicPartition]) = blocking {
        val partitionsJ = partitions.map(_.asJava).asJava
        consumer.seekToEnd(partitionsJ)
      }

      def position(partition: TopicPartition) = blocking {
        consumer.position(partition.asJava)
      }


      def position(partition: TopicPartition, timeout: FiniteDuration) = blocking {
        consumer.position(partition.asJava, timeout.asJava)
      }

      def committed(partition: TopicPartition) = blocking {
        val result = consumer.committed(partition.asJava)
        result.asScala
      }

      def committed(partition: TopicPartition, timeout: FiniteDuration) = blocking {
        val result = consumer.committed(partition.asJava, timeout.asJava)
        result.asScala
      }

      def partitions(topic: Topic) = blocking {
        val result = consumer.partitionsFor(topic)
        result.asScala.map(_.asScala).toList
      }

      def partitions(topic: Topic, timeout: FiniteDuration) = blocking {
        val result = consumer.partitionsFor(topic, timeout.asJava)
        result.asScala.map(_.asScala).toList
      }

      val listTopics = blocking {
        val result = consumer.listTopics()
        result.asScalaMap(k => k, _.asScala.map(_.asScala).toList)
      }

      def listTopics(timeout: FiniteDuration) = blocking {
        val result = consumer.listTopics(timeout.asJava)
        result.asScalaMap(k => k, _.asScala.map(_.asScala).toList)
      }

      def pause(partitions: Nel[TopicPartition]) = blocking {
        val partitionsJ = partitions.map(_.asJava).asJava
        consumer.pause(partitionsJ)
      }

      val paused = blocking {
        val partitionsJ = consumer.paused()
        partitionsJ.asScala.map(_.asScala).toSet
      }

      def resume(partitions: Nel[TopicPartition]) = blocking {
        val partitionsJ = partitions.map(_.asJava).asJava
        consumer.resume(partitionsJ)
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) = blocking {
        val result = consumer.offsetsForTimes(timestampsToSearch.asJavaMap(_.asJava, LongJ.valueOf))
        result.asScalaMap(_.asScala, v => Option(v).map(_.asScala))
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long], timeout: FiniteDuration) = blocking {
        val timestampsToSearchJ = timestampsToSearch.asJavaMap(_.asJava, LongJ.valueOf)
        val result = consumer.offsetsForTimes(timestampsToSearchJ, timeout.asJava)
        result.asScalaMap(_.asScala, v => Option(v).map(_.asScala))
      }

      def beginningOffsets(partitions: Nel[TopicPartition]) = blocking({
        val partitionsJ = partitions.map(_.asJava).asJava
        val result = consumer.beginningOffsets(partitionsJ)
        result.asScalaMap(_.asScala, v => v)
      })

      def beginningOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = blocking {
        val partitionsJ = partitions.map(_.asJava).asJava
        val result = consumer.beginningOffsets(partitionsJ, timeout.asJava)
        result.asScalaMap(_.asScala, v => v)
      }

      def endOffsets(partitions: Nel[TopicPartition]) = blocking {
        val partitionsJ = partitions.map(_.asJava).asJava
        val result = consumer.endOffsets(partitionsJ)
        result.asScalaMap(_.asScala, v => v)
      }

      def endOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = blocking {
        val partitionsJ = partitions.map(_.asJava).asJava
        val result = consumer.endOffsets(partitionsJ, timeout.asJava)
        result.asScalaMap(_.asScala, v => v)
      }

      val close = blocking {
        consumer.close()
      }

      def close(timeout: FiniteDuration) = blocking {
        consumer.close(timeout.asJava)
      }

      val wakeup = blocking {
        consumer.wakeup()
      }
    }
  }

  def apply[F[_] : Sync : ContextShift, K, V](
    consumer: Consumer[F, K, V],
    metrics: Metrics[F]): Consumer[F, K, V] = {

    def latencyForMetric[T](action: Consumer[F, K, V] => F[T])(measure: Metrics[F] => Long => F[Unit]): F[T] =
      for {
        time <- Sync[F].delay(Platform.currentTime)
        either <- action(consumer).attempt
        latency <- Sync[F].delay(Platform.currentTime - time)
        _ <- measure(metrics)(latency)
        result <- either match {
          case Right(result) => result.pure[F]
          case Left(error)   => error.raiseError[F, T]
        }
      } yield result

    val topics = consumer.assignment.map(_.map(_.topic).toList)

    def latencyFor[T](name: String, topics: Iterable[Topic])(f: F[T]): F[T] =
      for {
        time <- Sync[F].delay(Platform.currentTime)
        either <- f.attempt
        latency <- Sync[F].delay(Platform.currentTime - time)
        _ <- Traverse[List].traverse(topics.toList)(metrics.call(name, _, latency, either.isRight))
        result <- either match {
          case Right(result) => result.pure[F]
          case Left(error)   => error.raiseError[F, T]
        }
      } yield result

    implicit def nelToList[T](nel: Nel[T]): List[T] = nel.toList

    def latency[T](name: String)(f: F[T]): F[T] =
      topics.flatMap(latencyFor(name, _)(f))

    def count(name: String): F[Unit] =
      topics.flatMap(countFor(name, _))

    def countFor(name: String, topics: Iterable[Topic]): F[Unit] =
      Traverse[List].traverse(topics.toList)(metrics.count(name, _)) *> Applicative[F].unit

    def rebalanceListener(listener: RebalanceListener) = {

      def measure(name: String, partitions: Iterable[TopicPartition]) = {
        partitions.foreach { topicPartition =>
          metrics.rebalance(name, topicPartition)
        }
      }

      new RebalanceListener {

        def onPartitionsAssigned(partitions: Iterable[TopicPartition]) = {
          measure("assigned", partitions)
          listener.onPartitionsAssigned(partitions)
        }

        def onPartitionsRevoked(partitions: Iterable[TopicPartition]) = {
          measure("revoked", partitions)
          listener.onPartitionsRevoked(partitions)
        }
      }
    }

    new Consumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).to[Set]
        countFor("assign", topics) *> consumer.assign(partitions)
      }

      val assignment = consumer.assignment

      def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]) =
        countFor("subscribe", topics.to[List]) *> consumer.subscribe(topics, listener.map(rebalanceListener))


      def subscribe(pattern: Pattern, listener: Option[RebalanceListener]) = {
        countFor("subscribe", List("pattern")) *> consumer.subscribe(pattern, listener.map(rebalanceListener))
      }

      val subscription = consumer.subscription

      val unsubscribe =
        latency("unsubscribe") {
          consumer.unsubscribe
        }

      def poll(timeout: FiniteDuration) =
        for {
          records <- latency("poll") {
            consumer.poll(timeout)
          }
          topics <- Sync[F].delay(records.values.values.flatten.groupBy(_.topic))
          _ <- topics.toList.traverse {
            case (topic, topicRecords) =>
              val bytes = topicRecords.flatMap(_.value).map(_.serializedSize).sum
              metrics.poll(topic, bytes = bytes, records = topicRecords.size)
          }
        } yield records

      val commit =
        latency("commit") {
          consumer.commit
        }

      def commit(timeout: FiniteDuration) =
        latency("commit") {
          consumer.commit(timeout)
        }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = latencyFor("commit", offsets.keySet.map(_.topic)) {
        consumer.commit(offsets)
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = latencyFor("commit", offsets.keySet.map(_.topic)) {
        consumer.commit(offsets, timeout)
      }

      val commitLater = latency("commit_later") {
        consumer.commitLater
      }

      def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        latencyFor("commit_later", offsets.keySet.map(_.topic)) {
          consumer.commitLater(offsets)
        }
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        countFor("seek", Set(partition.topic))
        consumer.seek(partition, offset)
      }

      def seekToBeginning(partitions: Nel[TopicPartition]) =
        countFor("seek_to_beginning", partitions.map(_.topic).toList) *> consumer.seekToBeginning(partitions)

      def seekToEnd(partitions: Nel[TopicPartition]) =
        countFor("seek_to_end", partitions.map(_.topic).toList) *> consumer.seekToEnd(partitions)

      def position(partition: TopicPartition) =
        countFor("position", List(partition.topic)) *> consumer.position(partition)

      def position(partition: TopicPartition, timeout: FiniteDuration) =
        countFor("position", List(partition.topic)) *> consumer.position(partition, timeout)

      def committed(partition: TopicPartition) = {
        countFor("committed", List(partition.topic)) *> consumer.committed(partition)
      }

      def committed(partition: TopicPartition, timeout: FiniteDuration) =
        countFor("committed", List(partition.topic)) *> consumer.committed(partition, timeout)

      def partitions(topic: Topic) =
        countFor("partitions", List(topic)) *> consumer.partitions(topic)

      def partitions(topic: Topic, timeout: FiniteDuration) =
        countFor("partitions", List(topic)) *> consumer.partitions(topic, timeout)

      val listTopics = latencyForMetric(_.listTopics)(_.listTopics)

      def listTopics(timeout: FiniteDuration) = latencyForMetric(_.listTopics(timeout))(_.listTopics)

      def pause(partitions: Nel[TopicPartition]) =
        countFor("pause", partitions.map(_.topic).toList) *> consumer.pause(partitions)

      val paused = consumer.paused

      def resume(partitions: Nel[TopicPartition]) =
        countFor("resume", partitions.map(_.topic).toList) *> consumer.resume(partitions)

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) =
        latencyFor("offsets_for_times", timestampsToSearch.keySet.map(_.topic)) {
          consumer.offsetsForTimes(timestampsToSearch)
        }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long], timeout: FiniteDuration) =
        latencyFor("offsets_for_times", timestampsToSearch.keySet.map(_.topic)) {
          consumer.offsetsForTimes(timestampsToSearch, timeout)
        }

      def beginningOffsets(partitions: Nel[TopicPartition]) =
        latencyFor("beginning_offsets", partitions.map(_.topic)) {
          consumer.beginningOffsets(partitions)
        }

      def beginningOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) =
        latencyFor("beginning_offsets", partitions.map(_.topic).toList) {
          consumer.beginningOffsets(partitions, timeout)
        }

      def endOffsets(partitions: Nel[TopicPartition]) =
        latencyFor("end_offsets", partitions.map(_.topic).toList) {
          consumer.endOffsets(partitions)
        }

      def endOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) =
        latencyFor("end_offsets", partitions.map(_.topic)) {
          consumer.endOffsets(partitions, timeout)
        }

      val close =
        latency("close") {
          consumer.close
        }

      def close(timeout: FiniteDuration) =
        latency("close") {
          consumer.close(timeout)
        }

      val wakeup = count("wakeup") *> consumer.wakeup
    }
  }

}

