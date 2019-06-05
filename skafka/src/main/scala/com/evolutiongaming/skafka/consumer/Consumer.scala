package com.evolutiongaming.skafka
package consumer

import java.lang.{Long => LongJ}
import java.util.{Map => MapJ}
import java.util.regex.Pattern

import cats.effect.{Async, Clock, ContextShift, Resource, Sync}
import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import com.evolutiongaming.catshelper.ClockHelper._
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}
import org.apache.kafka.clients.consumer.{OffsetCommitCallback, Consumer => ConsumerJ}

import scala.collection.JavaConverters._
import scala.collection.immutable.Iterable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace

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

  def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): F[Unit]
  

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

  def wakeup: F[Unit]
}


object Consumer {

  def empty[F[_] : Applicative, K, V]: Consumer[F, K, V] = {

    val empty = ().pure[F]
    
    new Consumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = empty

      val assignment = Set.empty[TopicPartition].pure[F]

      def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]) = empty

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener]) = empty

      val subscription: F[Set[Topic]] = Set.empty[Topic].pure[F]

      val unsubscribe = empty

      def poll(timeout: FiniteDuration) = ConsumerRecords.empty[K, V].pure[F]

      val commit = empty

      def commit(timeout: FiniteDuration) = empty

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = empty

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = empty

      val commitLater = Map.empty[TopicPartition, OffsetAndMetadata].pure[F]

      def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]) = empty

      def seek(partition: TopicPartition, offset: Offset) = empty

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) = empty

      def seekToBeginning(partitions: Nel[TopicPartition]) = empty

      def seekToEnd(partitions: Nel[TopicPartition]) = empty

      def position(partition: TopicPartition) = Offset.Min.pure[F]

      def position(partition: TopicPartition, timeout: FiniteDuration) = Offset.Min.pure[F]

      def committed(partition: TopicPartition) = OffsetAndMetadata.empty.pure[F]

      def committed(partition: TopicPartition, timeout: FiniteDuration) = OffsetAndMetadata.empty.pure[F]

      def partitions(topic: Topic) = List.empty[PartitionInfo].pure[F]

      def partitions(topic: Topic, timeout: FiniteDuration) = List.empty[PartitionInfo].pure[F]

      val listTopics = Map.empty[Topic, List[PartitionInfo]].pure[F]

      def listTopics(timeout: FiniteDuration) = Map.empty[Topic, List[PartitionInfo]].pure[F]

      def pause(partitions: Nel[TopicPartition]) = empty

      val paused = Set.empty[TopicPartition].pure[F]

      def resume(partitions: Nel[TopicPartition]) = empty

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) = {
        Map.empty[TopicPartition, Option[OffsetAndTimestamp]].pure[F]
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration) = {
        Map.empty[TopicPartition, Option[OffsetAndTimestamp]].pure[F]
      }

      def beginningOffsets(partitions: Nel[TopicPartition]) = {
        Map.empty[TopicPartition, Offset].pure[F]
      }

      def beginningOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = {
        Map.empty[TopicPartition, Offset].pure[F]
      }

      def endOffsets(partitions: Nel[TopicPartition]) = {
        Map.empty[TopicPartition, Offset].pure[F]
      }

      def endOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = {
        Map.empty[TopicPartition, Offset].pure[F]
      }

      val wakeup = empty
    }
  }


  def of[F[_] : Async : ContextShift, K, V](
    config: ConsumerConfig,
    executorBlocking: ExecutionContext)(implicit
    valueFromBytes: FromBytes[V],
    keyFromBytes: FromBytes[K]
  ): Resource[F, Consumer[F, K, V]] = {

    val blocking = Blocking(executorBlocking)
    val consumerJ = CreateConsumerJ(config, valueFromBytes, keyFromBytes)

    val result = for {
      consumerJ <- consumerJ
      consumer   = apply(consumerJ, blocking)
      release    = blocking { consumerJ.close() }
    } yield {
      (consumer, release)
    }
    Resource(result)
  }


  def apply[F[_] : Async : ContextShift, K, V](
    consumer: ConsumerJ[K, V],
    blocking: Blocking[F]
  ): Consumer[F, K, V] = {

    def commitLater1(f: OffsetCommitCallback => Unit) = {
      val commitLater = Async[F].asyncF[MapJ[TopicPartitionJ, OffsetAndMetadataJ]] { f1 =>
        val callback = new OffsetCommitCallback {

          def onComplete(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], exception: Exception) = {
            if (exception != null) {
              f1(exception.asLeft)
            } else if (offsets != null) {
              f1(offsets.asRight)
            } else {
              val failure = new RuntimeException("both offsets & exception are nulls") with NoStackTrace
              f1(failure.asLeft)
            }
          }
        }

        blocking { f(callback) }
      }

      for {
        result <- commitLater
        _      <- ContextShift[F].shift
      } yield result
    }

    new Consumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.toList.map(_.asJava).asJavaCollection
        Sync[F].delay { consumer.assign(partitionsJ) }
      }

      val assignment = {
        for {
          result <- Sync[F].delay { consumer.assignment() }
        } yield {
          result.asScala.map(_.asScala).toSet
        }
      }

      def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]) = {
        val topicsJ = topics.asJava
        val listenerJ = (listener getOrElse RebalanceListener.empty).asJava
        Sync[F].delay { consumer.subscribe(topicsJ, listenerJ) }
      }

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener]) = {
        val listenerJ = (listener getOrElse RebalanceListener.empty).asJava
        Sync[F].delay { consumer.subscribe(pattern, listenerJ) }
      }

      val subscription = {
        for {
          result <- Sync[F].delay { consumer.subscription() }
        } yield {
          result.asScala.toSet
        }
      }

      val unsubscribe = {
        blocking { consumer.unsubscribe() }
      }

      def poll(timeout: FiniteDuration) = {
        val timeoutJ = timeout.asJava
        for {
          result <- blocking { consumer.poll(timeoutJ) }
        } yield {
          result.asScala
        }
      }

      val commit = {
        blocking { consumer.commitSync() }
      }

      def commit(timeout: FiniteDuration) = {
        val timeoutJ = timeout.asJava
        blocking { consumer.commitSync(timeoutJ) }
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        val offsetsJ = offsets.asJavaMap(_.asJava, _.asJava)
        blocking { consumer.commitSync(offsetsJ) }
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = {
        val offsetsJ = offsets.asJavaMap(_.asJava, _.asJava)
        val timeoutJ = timeout.asJava
        blocking { consumer.commitSync(offsetsJ, timeoutJ) }
      }

      val commitLater = {
        for {
          result <- commitLater1 { callback => consumer.commitAsync(callback) }
        } yield {
          result.asScalaMap(_.asScala, _.asScala)
        }
      }

      def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        val offsetsJ = offsets.deepAsJava
        commitLater1 { callback => consumer.commitAsync(offsetsJ, callback) }.void
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        val partitionsJ = partition.asJava
        Sync[F].delay { consumer.seek(partitionsJ, offset) }
      }

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) = {
        val partitionsJ = partition.asJava
        val offsetAndMetadataJ = offsetAndMetadata.asJava
        Sync[F].delay { consumer.seek(partitionsJ, offsetAndMetadataJ) }
      }

      def seekToBeginning(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        Sync[F].delay { consumer.seekToBeginning(partitionsJ) }
      }

      def seekToEnd(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        Sync[F].delay { consumer.seekToEnd(partitionsJ) }
      }

      def position(partition: TopicPartition) = {
        val partitionsJ = partition.asJava
        blocking { consumer.position(partitionsJ) }
      }


      def position(partition: TopicPartition, timeout: FiniteDuration) = {
        val partitionJ = partition.asJava
        val timeoutJ = timeout.asJava
        blocking { consumer.position(partitionJ, timeoutJ) }
      }

      def committed(partition: TopicPartition) = {
        val partitionJ = partition.asJava
        for {
          result <- blocking { consumer.committed(partitionJ) }
        } yield {
          result.asScala
        }
      }

      def committed(partition: TopicPartition, timeout: FiniteDuration) = {
        val partitionJ = partition.asJava
        val timeoutJ = timeout.asJava
        for {
          result <- blocking { consumer.committed(partitionJ, timeoutJ) }
        } yield {
          result.asScala
        }
      }

      def partitions(topic: Topic) = {
        for {
          result <- blocking { consumer.partitionsFor(topic) }
        } yield {
          result.asScala.map(_.asScala).toList
        }
      }

      def partitions(topic: Topic, timeout: FiniteDuration) = {
        val timeoutJ = timeout.asJava
        for {
          result <- blocking { consumer.partitionsFor(topic, timeoutJ) }
        } yield {
          result.asScala.map(_.asScala).toList
        }
      }

      val listTopics = {
        for {
          result <- blocking { consumer.listTopics() }
        } yield {
          result.asScalaMap(k => k, _.asScala.map(_.asScala).toList)
        }
      }

      def listTopics(timeout: FiniteDuration) = {
        val timeoutJ = timeout.asJava
        for {
          result <- blocking { consumer.listTopics(timeoutJ) }
        } yield {
          result.asScalaMap(k => k, _.asScala.map(_.asScala).toList)
        }
      }

      def pause(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        Sync[F].delay { consumer.pause(partitionsJ) }
      }

      val paused = {
        for {
          partitionsJ <- Sync[F].delay { consumer.paused() }
        } yield {
          partitionsJ.asScala.map(_.asScala).toSet
        }
      }

      def resume(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        Sync[F].delay { consumer.resume(partitionsJ) }
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) = {
        val timestampsToSearchJ = timestampsToSearch.asJavaMap(_.asJava, LongJ.valueOf)
        for {
          result <- blocking { consumer.offsetsForTimes(timestampsToSearchJ) }
        } yield {
          result.asScalaMap(_.asScala, v => Option(v).map(_.asScala))
        }
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long], timeout: FiniteDuration) = {
        val timestampsToSearchJ = timestampsToSearch.asJavaMap(_.asJava, LongJ.valueOf)
        val timeoutJ = timeout.asJava
        for {
          result <- blocking { consumer.offsetsForTimes(timestampsToSearchJ, timeoutJ) }
        } yield {
          result.asScalaMap(_.asScala, v => Option(v).map(_.asScala))
        }
      }

      def beginningOffsets(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        for {
          result <- blocking { consumer.beginningOffsets(partitionsJ) }
        } yield {
          result.asScalaMap(_.asScala, v => v)
        }
      }

      def beginningOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        val timeoutJ = timeout.asJava
        for {
          result <- blocking { consumer.beginningOffsets(partitionsJ, timeoutJ) }
        } yield {
          result.asScalaMap(_.asScala, v => v)
        }
      }

      def endOffsets(partitions: Nel[TopicPartition]) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        for {
          result <- blocking { consumer.endOffsets(partitionsJ) }
        } yield {
          result.asScalaMap(_.asScala, v => v)
        }
      }

      def endOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = {
        val partitionsJ = partitions.map(_.asJava).asJava
        val timeoutJ = timeout.asJava
        for {
          result <- blocking { consumer.endOffsets(partitionsJ, timeoutJ) }
        } yield {
          result.asScalaMap(_.asScala, v => v)
        }
      }
      
      val wakeup = {
        blocking { consumer.wakeup() }
      }
    }
  }
  

  def apply[F[_] : Sync : Clock, K, V](
    consumer: Consumer[F, K, V],
    metrics: Metrics[F]
  ): Consumer[F, K, V] = {

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    def latency1[A](fa: F[A]) = {
      for {
        start   <- Clock[F].millis
        result  <- fa
        end     <- Clock[F].millis
        latency  = end - start
      } yield {
        (result, latency)
      }
    }


    val topics = for {
      topicPartitions <- consumer.assignment
    } yield for {
      topicPartition <- topicPartitions
    } yield {
      topicPartition.topic
    }


    def call[A](name: String, topics: Iterable[Topic])(fa: F[A]): F[A] = {
      for {
        rl     <- latency1 { fa.attempt }
        (r, l)  = rl
        _      <- topics.toList.foldMap { topic => metrics.call(name, topic, l, r.isRight) }
        r      <- r.raiseOrPure[F]
      } yield r
    }

    def call1[T](name: String)(f: F[T]): F[T] = {
      for {
        topics <- topics
        r      <- call(name, topics)(f)
      } yield r
    }


    def count(name: String, topics: Iterable[Topic]) = {
      topics.toList.foldMapM { topic => metrics.count(name, topic) }
    }

    def count1(name: String): F[Unit] = {
      for {
        topics <- topics
        r      <- count(name, topics)
      } yield r
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

    new Consumer[F, K, V] {

      def assign(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).to[Set]
        for {
          _ <- count("assign", topics)
          r <- consumer.assign(partitions)
        } yield r
      }

      val assignment = consumer.assignment

      def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]) = {
        val listener1 = listener.map(rebalanceListener)
        for {
          _ <- count("subscribe", topics.to[List])
          r <- consumer.subscribe(topics, listener1)
        } yield r
      }


      def subscribe(pattern: Pattern, listener: Option[RebalanceListener]) = {
        val listener1 = listener.map(rebalanceListener)
        for {
          _ <- count("subscribe", List("pattern"))
          r <- consumer.subscribe(pattern, listener1)
        } yield r
      }

      val subscription = consumer.subscription

      val unsubscribe = {
        call1("unsubscribe") { consumer.unsubscribe }
      }

      def poll(timeout: FiniteDuration) =
        for {
          records <- call1("poll") {
            consumer.poll(timeout)
          }
          topics <- Sync[F].delay { records.values.values.flatten.groupBy(_.topic) }
          _ <- topics.toList.traverse {
            case (topic, topicRecords) =>
              val bytes = topicRecords.flatMap(_.value).map(_.serializedSize).sum
              metrics.poll(topic, bytes = bytes, records = topicRecords.size)
          }
        } yield records

      val commit = {
        call1("commit") { consumer.commit }
      }

      def commit(timeout: FiniteDuration) = {
        call1("commit") { consumer.commit(timeout) }
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        val topics = offsets.keySet.map(_.topic)
        call("commit", topics) { consumer.commit(offsets) }
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = {
        val topics = offsets.keySet.map(_.topic)
        call("commit", topics) { consumer.commit(offsets, timeout) }
      }

      val commitLater = call1("commit_later") {
        consumer.commitLater
      }

      def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        val topics = offsets.keySet.map(_.topic)
        call("commit_later", topics) { consumer.commitLater(offsets) }
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        for {
          _ <- count("seek", List(partition.topic))
          r <- consumer.seek(partition, offset)
        } yield r
      }

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) = {
        for {
          _ <- count("seek", List(partition.topic))
          r <- consumer.seek(partition, offsetAndMetadata)
        } yield r
      }

      def seekToBeginning(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).toList
        for {
          _ <- count("seek_to_beginning", topics)
          r <- consumer.seekToBeginning(partitions)
        } yield r
      }

      def seekToEnd(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).toList
        for {
          _ <- count("seek_to_end", topics)
          r <- consumer.seekToEnd(partitions)
        } yield r
      }

      def position(partition: TopicPartition) = {
        for {
          _ <- count("position", List(partition.topic))
          r <- consumer.position(partition)
        } yield r
      }

      def position(partition: TopicPartition, timeout: FiniteDuration) = {
        for {
          _ <- count("position", List(partition.topic))
          r <- consumer.position(partition, timeout)
        } yield r
      }

      def committed(partition: TopicPartition) = {
        for {
          _ <- count("committed", List(partition.topic))
          r <- consumer.committed(partition)
        } yield r
      }

      def committed(partition: TopicPartition, timeout: FiniteDuration) = {
        for {
          _ <- count("committed", List(partition.topic))
          r <- consumer.committed(partition, timeout)
        } yield r
      }

      def partitions(topic: Topic) = {
        for {
          _ <- count("partitions", List(topic))
          r <- consumer.partitions(topic)
        } yield r
      }

      def partitions(topic: Topic, timeout: FiniteDuration) = {
        for {
          _ <- count("partitions", List(topic))
          r <- consumer.partitions(topic, timeout)
        } yield r
      }

      val listTopics = {
        for {
          rl     <- latency1 { consumer.listTopics.attempt }
          (r, l)  = rl
          _      <- metrics.listTopics(l)
          r      <- r.raiseOrPure[F]
        } yield r
      }

      def listTopics(timeout: FiniteDuration) = {
        for {
          rl     <- latency1 { consumer.listTopics(timeout).attempt }
          (r, l)  = rl
          _      <- metrics.listTopics(l)
          r      <- r.raiseOrPure[F]
        } yield r
      }

      def pause(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).toList
        for {
          _ <- count("pause", topics)
          r <- consumer.pause(partitions)
        } yield r
      }

      val paused = consumer.paused

      def resume(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).toList
        for {
          _ <- count("resume", topics)
          r <- consumer.resume(partitions)
        } yield r
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) = {
        val topics = timestampsToSearch.keySet.map(_.topic)
        call("offsets_for_times", topics) { consumer.offsetsForTimes(timestampsToSearch) }
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long], timeout: FiniteDuration) = {
        val topics = timestampsToSearch.keySet.map(_.topic)
        call("offsets_for_times", topics) { consumer.offsetsForTimes(timestampsToSearch, timeout) }
      }

      def beginningOffsets(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).toList
        call("beginning_offsets", topics) { consumer.beginningOffsets(partitions) }
      }

      def beginningOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = {
        val topics = partitions.map(_.topic).toList
        call("beginning_offsets", topics) { consumer.beginningOffsets(partitions, timeout) }
      }

      def endOffsets(partitions: Nel[TopicPartition]) = {
        val topics = partitions.map(_.topic).toList
        call("end_offsets", topics) { consumer.endOffsets(partitions) }
      }

      def endOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = {
        val topics = partitions.map(_.topic).toList
        call("end_offsets", topics) { consumer.endOffsets(partitions, timeout) }
      }

      val wakeup = {
        for {
          _ <- count1("wakeup")
          r <- consumer.wakeup
        } yield r
      }
    }
  }


  implicit class ConsumerOps[F[_], K, V](val self: Consumer[F, K, V]) extends AnyVal {

    def withMetrics(metrics: Metrics[F])(implicit F: Sync[F], clock: Clock[F]): Consumer[F, K, V] = {
      Consumer(self, metrics)
    }

    def mapK[G[_]](f: F ~> G): Consumer[G, K, V] = new Consumer[G, K, V] {

      def assign(partitions: Nel[TopicPartition]) = f(self.assign(partitions))

      def assignment = f(self.assignment)

      def subscribe(topics: Nel[Topic], listener: Option[RebalanceListener]) = f(self.subscribe(topics, listener))

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener]) = f(self.subscribe(pattern, listener))

      def subscription = f(self.subscription)

      def unsubscribe = f(self.unsubscribe)

      def poll(timeout: FiniteDuration) = f(self.poll(timeout))

      def commit = f(self.commit)

      def commit(timeout: FiniteDuration) = f(self.commit(timeout))

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = f(self.commit(offsets))

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = f(self.commit(offsets, timeout))

      def commitLater = f(self.commitLater)

      def commitLater(offsets: Map[TopicPartition, OffsetAndMetadata]) = f(self.commitLater(offsets))

      def seek(partition: TopicPartition, offset: Offset) = f(self.seek(partition, offset))

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) = f(self.seek(partition, offsetAndMetadata))

      def seekToBeginning(partitions: Nel[TopicPartition]) = f(self.seekToBeginning(partitions))

      def seekToEnd(partitions: Nel[TopicPartition]) = f(self.seekToEnd(partitions))

      def position(partition: TopicPartition) = f(self.position(partition))

      def position(partition: TopicPartition, timeout: FiniteDuration) = f(self.position(partition, timeout))

      def committed(partition: TopicPartition) = f(self.committed(partition))

      def committed(partition: TopicPartition, timeout: FiniteDuration) = f(self.committed(partition, timeout))

      def partitions(topic: Topic) = f(self.partitions(topic))

      def partitions(topic: Topic, timeout: FiniteDuration) = f(self.partitions(topic, timeout))

      def listTopics = f(self.listTopics)

      def listTopics(timeout: FiniteDuration) = f(self.listTopics(timeout))

      def pause(partitions: Nel[TopicPartition]) = f(self.pause(partitions))

      def paused = f(self.paused)

      def resume(partitions: Nel[TopicPartition]) = f(self.resume(partitions))

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) = {
        f(self.offsetsForTimes(timestampsToSearch))
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration) = {
        f(self.offsetsForTimes(timestampsToSearch, timeout))
      }

      def beginningOffsets(partitions: Nel[TopicPartition]) = f(self.beginningOffsets(partitions))

      def beginningOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = {
        f(self.beginningOffsets(partitions, timeout))
      }

      def endOffsets(partitions: Nel[TopicPartition]) = {
        f(self.endOffsets(partitions))
      }

      def endOffsets(partitions: Nel[TopicPartition], timeout: FiniteDuration) = {
        f(self.endOffsets(partitions, timeout))
      }

      def wakeup = f(self.wakeup)
    }
  }
}

