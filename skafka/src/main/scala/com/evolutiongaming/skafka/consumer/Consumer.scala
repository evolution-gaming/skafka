package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.util.regex.Pattern

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import org.apache.kafka.clients.consumer.{KafkaConsumer, Consumer => ConsumerJ}

import scala.collection.JavaConverters._
import scala.collection.immutable.Iterable
import scala.compat.Platform
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal

/**
  * See [[org.apache.kafka.clients.consumer.Consumer]]
  */
trait Consumer[K, V] {

  def assign(partitions: Nel[TopicPartition]): Unit

  def assignment(): Set[TopicPartition]

  def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]): Unit

  def subscribe(pattern: Pattern, listener: Option[RebalanceListener]): Unit

  def subscription(): Set[Topic]

  def unsubscribe(): Future[Unit]

  def poll(timeout: FiniteDuration): Future[ConsumerRecords[K, V]]

  def commit(): Future[Unit]

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Unit]

  def commitLater(): Future[Map[TopicPartition, OffsetAndMetadata]]

  def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]): Future[Unit]

  def seek(partition: TopicPartition, offset: Offset): Unit

  def seekToBeginning(partitions: Nel[TopicPartition]): Unit

  def seekToEnd(partitions: Nel[TopicPartition]): Unit

  def position(partition: TopicPartition): Future[Offset]

  def committed(partition: TopicPartition): Future[OffsetAndMetadata]

  def partitionsFor(topic: Topic): Future[List[PartitionInfo]]

  def listTopics(): Future[Map[Topic, List[PartitionInfo]]]

  def pause(partitions: Nel[TopicPartition]): Unit

  def paused(): Set[TopicPartition]

  def resume(partitions: Nel[TopicPartition]): Unit

  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]): Future[Map[TopicPartition, Option[OffsetAndTimestamp]]]

  def beginningOffsets(partitions: Nel[TopicPartition]): Future[Map[TopicPartition, Offset]]

  def endOffsets(partitions: Nel[TopicPartition]): Future[Map[TopicPartition, Offset]]

  def close(): Future[Unit]

  def close(timeout: FiniteDuration): Future[Unit]

  def wakeup(): Future[Unit]
}


object Consumer {

  def empty[K, V]: Consumer[K, V] = new Consumer[K, V] {

    def assign(partitions: Nel[TopicPartition]) = {}

    def assignment() = Set.empty

    def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]) = {}

    def subscribe(pattern: Pattern, listener: Option[RebalanceListener]) = {}

    def subscription() = Set.empty

    def unsubscribe() = Future.unit

    def poll(timeout: FiniteDuration) = ConsumerRecords.empty[K, V].future

    def commit() = Future.unit

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = Future.unit

    def commitLater() = Map.empty[TopicPartition, OffsetAndMetadata].future

    def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]) = Future.unit

    def seek(partition: TopicPartition, offset: Offset) = {}

    def seekToBeginning(partitions: Nel[TopicPartition]) = {}

    def seekToEnd(partitions: Nel[TopicPartition]) = {}

    def position(partition: TopicPartition) = Offset.Min.future

    def committed(partition: TopicPartition) = OffsetAndMetadata.Empty.future

    def partitionsFor(topic: Topic) = Future.nil

    def listTopics() = Map.empty[Topic, List[PartitionInfo]].future

    def pause(partitions: Nel[TopicPartition]) = {}

    def paused() = Set.empty

    def resume(partitions: Nel[TopicPartition]) = {}

    def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) = Map.empty[TopicPartition, Option[OffsetAndTimestamp]].future

    def beginningOffsets(partitions: Nel[TopicPartition]) = Map.empty[TopicPartition, Offset].future

    def endOffsets(partitions: Nel[TopicPartition]) = Map.empty[TopicPartition, Offset].future

    def close() = Future.unit

    def close(timeout: FiniteDuration) = Future.unit

    def wakeup() = Future.unit
  }


  def apply[K, V](
    config: ConsumerConfig,
    ecBlocking: ExecutionContext)(implicit
    valueFromBytes: FromBytes[V],
    keyFromBytes: FromBytes[K]): Consumer[K, V] = {

    val valueDeserializer = valueFromBytes.asJava
    val keyDeserializer = keyFromBytes.asJava
    val consumer = new KafkaConsumer(config.properties, keyDeserializer, valueDeserializer)
    apply(consumer, ecBlocking)
  }

  def apply[K, V](consumer: ConsumerJ[K, V], ecBlocking: ExecutionContext): Consumer[K, V] = {

    def blocking[T](f: => T): Future[T] = Future(f)(ecBlocking)

    def callbackAndFuture() = {
      val promise = Promise[Map[TopicPartition, OffsetAndMetadata]]()
      val callback = new CommitCallback {
        def apply(offsets: Try[Map[TopicPartition, OffsetAndMetadata]]) = {
          promise.complete(offsets)
        }
      }
      (callback, promise.future)
    }

    new Consumer[K, V] {

      def assign(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.toList.map(_.asJava).asJavaCollection
        consumer.assign(partitionsJ)
      }

      def assignment(): Set[TopicPartition] = {
        val partitionsJ = consumer.assignment()
        partitionsJ.asScala.map(_.asScala).toSet
      }

      def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]) = {
        val topicsJ = topics.asJava
        consumer.subscribe(topicsJ, (listener getOrElse RebalanceListener.Empty).asJava)
      }

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener]) = {
        consumer.subscribe(pattern, (listener getOrElse RebalanceListener.Empty).asJava)
      }

      def subscription() = {
        consumer.subscription().asScala.toSet
      }

      def unsubscribe() = {
        blocking {
          consumer.unsubscribe()
        }
      }

      def poll(timeout: FiniteDuration): Future[ConsumerRecords[K, V]] = {
        blocking {
          val records = consumer.poll(timeout.toMillis)
          records.asScala
        }
      }

      def commit() = {
        blocking {
          consumer.commitSync()
        }
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        val offsetsJ = offsets.asJavaMap(_.asJava, _.asJava)
        blocking {
          consumer.commitSync(offsetsJ)
        }
      }

      def commitLater() = {
        try {
          val (callback, future) = callbackAndFuture()
          consumer.commitAsync(callback.asJava)
          future
        } catch {
          case NonFatal(failure) => Future.failed(failure)
        }
      }

      def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        try {
          val (callback, future) = callbackAndFuture()
          val offsetsJ = offsets.asJavaMap(_.asJava, _.asJava)
          consumer.commitAsync(offsetsJ, callback.asJava)
          future.unit
        } catch {
          case NonFatal(failure) => Future.failed(failure)
        }
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        consumer.seek(partition.asJava, offset)
      }

      def seekToBeginning(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        consumer.seekToBeginning(partitionsJ)
      }

      def seekToEnd(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        consumer.seekToEnd(partitionsJ)
      }

      def position(partition: TopicPartition) = {
        blocking {
          consumer.position(partition.asJava)
        }
      }

      def committed(partition: TopicPartition) = {
        val partitionJ = partition.asJava
        blocking {
          val result = consumer.committed(partitionJ)
          result.asScala
        }
      }

      def partitionsFor(topic: Topic) = {
        blocking {
          val result = consumer.partitionsFor(topic)
          result.asScala.map(_.asScala).toList
        }
      }

      def listTopics() = {
        blocking {
          val result = consumer.listTopics()
          result.asScalaMap(k => k, _.asScala.map(_.asScala).toList)
        }
      }

      def pause(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        consumer.pause(partitionsJ)
      }

      def paused(): Set[TopicPartition] = {
        val partitionsJ = consumer.paused()
        partitionsJ.asScala.map(_.asScala).toSet
      }

      def resume(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        consumer.resume(partitionsJ)
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) = {
        val timestampsToSearchJ = timestampsToSearch.asJavaMap(_.asJava, LongJ.valueOf)
        blocking {
          val result = consumer.offsetsForTimes(timestampsToSearchJ)
          result.asScalaMap(_.asScala, v => Option(v).map(_.asScala))
        }
      }

      def beginningOffsets(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        blocking {
          val result = consumer.beginningOffsets(partitionsJ)
          result.asScalaMap(_.asScala, v => v)
        }
      }

      def endOffsets(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        blocking {
          val result = consumer.endOffsets(partitionsJ)
          result.asScalaMap(_.asScala, v => v)
        }
      }

      def close() = {
        blocking {
          consumer.close()
        }
      }

      def close(timeout: FiniteDuration) = {
        blocking {
          consumer.close(timeout.length, timeout.unit)
        }
      }

      def wakeup() = {
        blocking {
          consumer.wakeup()
        }
      }
    }
  }


  def apply[K, V](consumer: Consumer[K, V], metrics: Metrics): Consumer[K, V] = {

    implicit val ec = CurrentThreadExecutionContext

    def latency[T](name: String, topics: Set[Topic])(f: => Future[T]): Future[T] = {
      val time = Platform.currentTime
      val result = f
      result.onComplete { result =>
        val latency = Platform.currentTime - time
        for {topic <- topics} {
          metrics.call(name = name, topic = topic, latency = latency, success = result.isSuccess)
        }
      }
      result
    }

    def count(name: String, topics: Set[Topic]) = {
      topics.foreach { topic =>
        metrics.count(name = name, topic = topic)
      }
    }


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

    def topics() = consumer.assignment().map(_.topic)

    new Consumer[K, V] {

      def assign(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).to[Set]
        count("assign", topics)
        consumer.assign(partitions)
      }

      def assignment() = consumer.assignment()

      def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]) = {
        count("subscribe", topics.to[Set])
        consumer.subscribe(topics, listener.map(rebalanceListener))
      }

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener]) = {
        count("subscribe", Set("pattern"))
        consumer.subscribe(pattern, listener.map(rebalanceListener))
      }

      def subscription() = consumer.subscription()

      def unsubscribe() = {
        latency("unsubscribe", topics()) {
          consumer.unsubscribe()
        }
      }

      def poll(timeout: FiniteDuration) = {
        val result = latency("poll", topics()) {
          consumer.poll(timeout)
        }

        for {
          records <- result
          (topic, records) <- records.values.values.flatten.groupBy(_.topic)
        } {
          val bytes = records.foldLeft(0) { (acc, record) => acc + record.value.fold(0)(_.serializedSize) }
          metrics.poll(topic, bytes = bytes, records = records.size)
        }

        result
      }

      def commit() = {
        latency("commit", topics()) {
          consumer.commit()
        }
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        latency("commit", offsets.keySet.map(_.topic)) {
          consumer.commit(offsets)
        }
      }

      def commitLater(): Future[Map[TopicPartition, OffsetAndMetadata]] = {
        latency("commit_later", topics()) {
          consumer.commitLater()
        }
      }

      def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        latency("commit_later", offsets.keySet.map(_.topic)) {
          consumer.commitLater(offsets)
        }
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        count("seek", Set(partition.topic))
        consumer.seek(partition, offset)
      }

      def seekToBeginning(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).to[Set]
        count("seek_to_beginning", topics)
        consumer.seekToBeginning(partitions)
      }

      def seekToEnd(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).to[Set]
        count("seek_to_end", topics)
        consumer.seekToEnd(partitions)
      }

      def position(partition: TopicPartition) = {
        latency("position", Set(partition.topic)) {
          consumer.position(partition)
        }
      }

      def committed(partition: TopicPartition) = {
        latency("committed", Set(partition.topic)) {
          consumer.committed(partition)
        }
      }

      def partitionsFor(topic: Topic) = {
        latency("partitions", Set(topic)) {
          consumer.partitionsFor(topic)
        }
      }

      def listTopics() = {
        val start = Platform.currentTime
        val result = consumer.listTopics()
        result.onComplete { _ =>
          val latency = Platform.currentTime - start
          metrics.listTopics(latency)
        }
        result
      }

      def pause(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).to[Set]
        count("pause", topics)
        consumer.pause(partitions)
      }

      def paused() = consumer.paused()

      def resume(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).to[Set]
        count("resume", topics)
        consumer.resume(partitions)
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) = {
        val topics = timestampsToSearch.keySet.map(_.topic)
        latency("offsets_for_times", topics) {
          consumer.offsetsForTimes(timestampsToSearch)
        }
      }

      def beginningOffsets(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).to[Set]
        latency("beginning_offsets", topics) {
          consumer.beginningOffsets(partitions)
        }
      }

      def endOffsets(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).to[Set]
        latency("end_offsets", topics) {
          consumer.endOffsets(partitions)
        }
      }

      def close() = {
        latency("close", topics()) {
          consumer.close()
        }
      }

      def close(timeout: FiniteDuration) = {
        latency("close", topics()) {
          consumer.close(timeout)
        }
      }

      def wakeup() = {
        count("wakeup", topics())
        consumer.wakeup()
      }
    }
  }


  trait Metrics {

    def call(name: String, topic: Topic, latency: Long, success: Boolean): Unit

    def poll(topic: Topic, bytes: Int, records: Int): Unit

    def count(name: String, topic: Topic): Unit

    def rebalance(name: String, topicPartition: TopicPartition): Unit

    def listTopics(latency: Long): Unit
  }

  object Metrics {

    val Empty: Metrics = new Metrics {

      def call(name: String, topic: Topic, latency: Offset, success: Boolean) = {}

      def poll(topic: Topic, bytes: Partition, records: Partition) = {}

      def count(name: String, topic: Topic) = {}

      def rebalance(name: String, topicPartition: TopicPartition) = {}

      def listTopics(latency: Offset) = {}
    }
  }
}

