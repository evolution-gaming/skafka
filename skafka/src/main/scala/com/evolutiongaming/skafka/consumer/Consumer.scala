package com.evolutiongaming.skafka
package consumer

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.effect.implicits._
import cats.implicits._
import cats.{Applicative, Monad, MonadError, ~>}
import com.evolutiongaming.catshelper.Blocking.implicits._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper._
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import com.evolutiongaming.smetrics.MeasureDuration
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{OffsetCommitCallback, Consumer => ConsumerJ, ConsumerRebalanceListener => ConsumerRebalanceListenerJ, OffsetAndMetadata => OffsetAndMetadataJ, OffsetAndTimestamp => OffsetAndTimestampJ}
import org.apache.kafka.common.{PartitionInfo => PartitionInfoJ, TopicPartition => TopicPartitionJ}

import java.lang.{Long => LongJ}
import java.util.regex.Pattern
import java.util.{Collection => CollectionJ, List => ListJ, Map => MapJ, Set => SetJ}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
  * See [[org.apache.kafka.clients.consumer.Consumer]]
  */
trait Consumer[F[_], K, V] {

  def assign(partitions: Nes[TopicPartition]): F[Unit]

  def assignment: F[Set[TopicPartition]]

  def subscribe(topics: Nes[Topic], listener: RebalanceListener1[F]): F[Unit]

  def subscribe(topics: Nes[Topic]): F[Unit]

  def subscribe(pattern: Pattern, listener: RebalanceListener1[F]): F[Unit]

  def subscribe(pattern: Pattern): F[Unit]

  @deprecated("please use subscribe with RebalanceListener1", "11.1.0")
  def subscribe(topics: Nes[Topic], listener: Option[RebalanceListener[F]]): F[Unit]

  @deprecated("please use subscribe with RebalanceListener1", "11.1.0")
  def subscribe(pattern: Pattern, listener: Option[RebalanceListener[F]]): F[Unit]

  def subscription: F[Set[Topic]]

  def unsubscribe: F[Unit]

  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]]

  def commit: F[Unit]

  def commit(timeout: FiniteDuration): F[Unit]

  def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]): F[Unit]

  def commit(offsets: Nem[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration): F[Unit]

  def commitLater: F[Map[TopicPartition, OffsetAndMetadata]]

  def commitLater(offsets: Nem[TopicPartition, OffsetAndMetadata]): F[Unit]

  def seek(partition: TopicPartition, offset: Offset): F[Unit]

  def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): F[Unit]

  def seekToBeginning(partitions: Nes[TopicPartition]): F[Unit]

  def seekToEnd(partitions: Nes[TopicPartition]): F[Unit]

  def position(partition: TopicPartition): F[Offset]

  def position(partition: TopicPartition, timeout: FiniteDuration): F[Offset]

  def committed(partitions: Nes[TopicPartition]): F[Map[TopicPartition, OffsetAndMetadata]]

  def committed(partitions: Nes[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, OffsetAndMetadata]]

  def partitions(topic: Topic): F[List[PartitionInfo]]

  def partitions(topic: Topic, timeout: FiniteDuration): F[List[PartitionInfo]]

  def topics: F[Map[Topic, List[PartitionInfo]]]

  def topics(timeout: FiniteDuration): F[Map[Topic, List[PartitionInfo]]]

  def pause(partitions: Nes[TopicPartition]): F[Unit]

  def paused: F[Set[TopicPartition]]

  def resume(partitions: Nes[TopicPartition]): F[Unit]

  def offsetsForTimes(
    timestampsToSearch: Map[TopicPartition, Offset]
  ): F[Map[TopicPartition, Option[OffsetAndTimestamp]]]

  def offsetsForTimes(
    timestampsToSearch: Map[TopicPartition, Offset],
    timeout: FiniteDuration
  ): F[Map[TopicPartition, Option[OffsetAndTimestamp]]]

  def beginningOffsets(partitions: Nes[TopicPartition]): F[Map[TopicPartition, Offset]]

  def beginningOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, Offset]]

  def endOffsets(partitions: Nes[TopicPartition]): F[Map[TopicPartition, Offset]]

  def endOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, Offset]]

  def groupMetadata: F[ConsumerGroupMetadata]

  def wakeup: F[Unit]
}

object Consumer {

  private sealed abstract class Empty

  def empty[F[_]: Applicative, K, V]: Consumer[F, K, V] = {

    val empty = ().pure[F]

    new Empty with Consumer[F, K, V] {

      def assign(partitions: Nes[TopicPartition]) = empty

      val assignment = Set.empty[TopicPartition].pure[F]

      def subscribe(topics: Nes[Topic], listener: RebalanceListener1[F]): F[Unit] = empty

      def subscribe(topics: Nes[Topic]): F[Unit] = empty

      def subscribe(pattern: Pattern, listener: RebalanceListener1[F]): F[Unit] = empty

      def subscribe(pattern: Pattern): F[Unit] = empty

      def subscribe(topics: Nes[Topic], listener: Option[RebalanceListener[F]]) = empty

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener[F]]) = empty

      val subscription: F[Set[Topic]] = Set.empty[Topic].pure[F]

      val unsubscribe = empty

      def poll(timeout: FiniteDuration) = ConsumerRecords.empty[K, V].pure[F]

      val commit = empty

      def commit(timeout: FiniteDuration) = empty

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]) = empty

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = empty

      val commitLater = Map.empty[TopicPartition, OffsetAndMetadata].pure[F]

      def commitLater(offsets: Nem[TopicPartition, OffsetAndMetadata]) = empty

      def seek(partition: TopicPartition, offset: Offset) = empty

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) = empty

      def seekToBeginning(partitions: Nes[TopicPartition]) = empty

      def seekToEnd(partitions: Nes[TopicPartition]) = empty

      def position(partition: TopicPartition) = Offset.min.pure[F]

      def position(partition: TopicPartition, timeout: FiniteDuration) = Offset.min.pure[F]

      def committed(partitions: Nes[TopicPartition]) = {
        Map.empty[TopicPartition, OffsetAndMetadata].pure[F]
      }

      def committed(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
        Map.empty[TopicPartition, OffsetAndMetadata].pure[F]
      }

      def partitions(topic: Topic) = List.empty[PartitionInfo].pure[F]

      def partitions(topic: Topic, timeout: FiniteDuration) = List.empty[PartitionInfo].pure[F]

      val topics = Map.empty[Topic, List[PartitionInfo]].pure[F]

      def topics(timeout: FiniteDuration) = Map.empty[Topic, List[PartitionInfo]].pure[F]

      def pause(partitions: Nes[TopicPartition]) = empty

      val paused = Set.empty[TopicPartition].pure[F]

      def resume(partitions: Nes[TopicPartition]) = empty

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) = {
        Map.empty[TopicPartition, Option[OffsetAndTimestamp]].pure[F]
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration) = {
        Map.empty[TopicPartition, Option[OffsetAndTimestamp]].pure[F]
      }

      def beginningOffsets(partitions: Nes[TopicPartition]) = {
        Map.empty[TopicPartition, Offset].pure[F]
      }

      def beginningOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
        Map.empty[TopicPartition, Offset].pure[F]
      }

      def endOffsets(partitions: Nes[TopicPartition]) = {
        Map.empty[TopicPartition, Offset].pure[F]
      }

      def endOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
        Map.empty[TopicPartition, Offset].pure[F]
      }

      def groupMetadata = ConsumerGroupMetadata.Empty.pure[F]

      val wakeup = empty
    }
  }


  private sealed abstract class Main

  def of[F[_]: Concurrent: ContextShift: ToTry: ToFuture, K, V](
    config: ConsumerConfig,
    executorBlocking: ExecutionContext
  )(implicit fromBytesK: FromBytes[F, K], fromBytesV: FromBytes[F, V]): Resource[F, Consumer[F, K, V]] = {
    implicit val blocking = Blocking.fromExecutionContext(executorBlocking)
    val consumer          = CreateConsumerJ(config, fromBytesK, fromBytesV)
    fromConsumerJ(consumer)
  }

  def fromConsumerJ[F[_]: Concurrent: ContextShift: ToFuture: ToTry: Blocking, K, V](
    consumer: F[ConsumerJ[K, V]]
  ): Resource[F, Consumer[F, K, V]] = {

    def blocking[A](f: => A) = Sync[F].delay { f }.blocking

    trait Around {
      def apply[A](f: => A): F[A]
    }

    for {
      serialListeners <- SerialListeners.of[F].toResource
      semaphore       <- Semaphore(1).toResource
      consumer        <- consumer.blocking.toResource
      around = new Around {
        def apply[A](f: => A) = {
          semaphore
            .withPermit { serialListeners.around { blocking { f } } }
            .flatten
            .uncancelable
        }
      }
      close = around { consumer.close() }
      _    <- Resource.release { close }
    } yield {

      def serial[A](fa: F[A]) = semaphore.withPermit(fa).uncancelable

      def serialNonBlocking[A](f: => A) = serial { Sync[F].delay { f } }

      def serialBlocking[A](f: => A) = serial { blocking { f } }

      def commitLater1(f: OffsetCommitCallback => Unit) = {
        Async[F]
          .async[MapJ[TopicPartitionJ, OffsetAndMetadataJ]] { callback =>
            val offsetCommitCallback = new OffsetCommitCallback {

              def onComplete(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], exception: Exception) = {
                if (exception != null) {
                  callback(exception.asLeft)
                } else if (offsets != null) {
                  callback(offsets.asRight)
                } else {
                  val failure = SkafkaError("both offsets & exception are nulls")
                  callback(failure.asLeft)
                }
              }
            }
            f(offsetCommitCallback)
          }
          .blocking
      }

      def listenerOf(listener: Option[RebalanceListener[F]]) = {
        listener.fold[ConsumerRebalanceListenerJ] {
          new NoOpConsumerRebalanceListener
        } { listener =>
          listener.asJava(serialListeners)
        }
      }

      def position1(partition: TopicPartition)(f: TopicPartitionJ => Long) = {
        val partitionJ = partition.asJava
        for {
          offset <- serialBlocking { f(partitionJ) }
          offset <- Offset.of[F](offset)
        } yield offset
      }

      def committed1(partitions: Nes[TopicPartition])(
        f: (SetJ[TopicPartitionJ]) => MapJ[TopicPartitionJ, OffsetAndMetadataJ]
      ) = {
        val partitionsJ = partitions.asJava
        for {
          result <- serialBlocking { f(partitionsJ) }
          result <- committedOffsetsF[F](result)
        } yield result
      }

      def partitions1(f: => ListJ[PartitionInfoJ]) = {
        for {
          result <- serialBlocking { f }
          result <- partitionsInfoListF[F](result)
        } yield result
      }

      def topics1(f: => MapJ[Topic, ListJ[PartitionInfoJ]]) = {
        for {
          result <- serialBlocking { f }
          result <- partitionsInfoMapF[F](result)
        } yield result
      }

      def offsetsForTimes1(timestamps: Map[TopicPartition, Offset])(
        f: MapJ[TopicPartitionJ, LongJ] => MapJ[TopicPartitionJ, OffsetAndTimestampJ]
      ) = {
        val timestampsJ = timestamps.asJavaMap(_.asJava, a => LongJ.valueOf(a.value))
        for {
          result <- serialBlocking { f(timestampsJ) }
          result <- offsetsAndTimestampsMapF[F](result)
        } yield result
      }

      def offsetsOf(partitions: Nes[TopicPartition])(
        f: CollectionJ[TopicPartitionJ] => MapJ[TopicPartitionJ, LongJ]
      ) = {
        val partitionsJ = partitions.asJava
        for {
          result <- serialBlocking { f(partitionsJ) }
          result <- offsetsMapF[F](result)
        } yield result
      }

      new Main with Consumer[F, K, V] {

        def assign(partitions: Nes[TopicPartition]) = {
          val partitionsJ = partitions.toList.map { _.asJava }.asJavaCollection
          serialNonBlocking { consumer.assign(partitionsJ) }
        }

        val assignment = {
          for {
            result <- serialNonBlocking { consumer.assignment() }
            result <- topicPartitionsSetF[F](result)
          } yield result
        }

        def subscribe(topics: Nes[Topic], listener: RebalanceListener1[F]) = {
          val topicsJ   = topics.toSortedSet.asJava
          val listenerJ = listener.asJava(consumer)
          serialNonBlocking { consumer.subscribe(topicsJ, listenerJ) }
        }

        def subscribe(topics: Nes[Topic]) = {
          val topicsJ = topics.toSortedSet.asJava
          serialNonBlocking { consumer.subscribe(topicsJ, new NoOpConsumerRebalanceListener) }
        }

        def subscribe(pattern: Pattern, listener: RebalanceListener1[F]) = {
          val listenerJ = listener.asJava(consumer)
          serialNonBlocking { consumer.subscribe(pattern, listenerJ) }
        }

        def subscribe(pattern: Pattern) = {
          serialNonBlocking { consumer.subscribe(pattern, new NoOpConsumerRebalanceListener) }
        }

        def subscribe(topics: Nes[Topic], listener: Option[RebalanceListener[F]]) = {
          val topicsJ   = topics.toSortedSet.toSet.asJava
          val listenerJ = listenerOf(listener)
          serialNonBlocking { consumer.subscribe(topicsJ, listenerJ) }
        }

        def subscribe(pattern: Pattern, listener: Option[RebalanceListener[F]]) = {
          val listenerJ = listenerOf(listener)
          serialNonBlocking { consumer.subscribe(pattern, listenerJ) }
        }

        val subscription = {
          for {
            result <- serialNonBlocking { consumer.subscription() }
          } yield {
            result.asScala.toSet
          }
        }

        val unsubscribe = {
          serialBlocking { consumer.unsubscribe() }
        }

        def poll(timeout: FiniteDuration) = {
          val timeoutJ = timeout.asJava
          around { consumer.poll(timeoutJ) }.flatMap { _.asScala[F] }
        }

        val commit = {
          serialBlocking { consumer.commitSync() }
        }

        def commit(timeout: FiniteDuration) = {
          val timeoutJ = timeout.asJava
          serialBlocking { consumer.commitSync(timeoutJ) }
        }

        def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]) = {
          val offsetsJ = asOffsetsAndMetadataJ(offsets)
          serialBlocking { consumer.commitSync(offsetsJ) }
        }

        def commit(offsets: Nem[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = {
          val offsetsJ = asOffsetsAndMetadataJ(offsets)
          val timeoutJ = timeout.asJava
          serialBlocking { consumer.commitSync(offsetsJ, timeoutJ) }
        }

        val commitLater = {
          for {
            result <- commitLater1 { callback => consumer.commitAsync(callback) }
            result <- result.asScalaMap(_.asScala[F], _.asScala[F])
          } yield result
        }

        def commitLater(offsets: Nem[TopicPartition, OffsetAndMetadata]) = {
          val offsetsJ = offsets.toSortedMap.deepAsJava
          commitLater1 { callback => consumer.commitAsync(offsetsJ, callback) }.void
        }

        def seek(partition: TopicPartition, offset: Offset) = {
          val partitionsJ = partition.asJava
          serialNonBlocking { consumer.seek(partitionsJ, offset.value) }
        }

        def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) = {
          val partitionsJ        = partition.asJava
          val offsetAndMetadataJ = offsetAndMetadata.asJava
          serialNonBlocking { consumer.seek(partitionsJ, offsetAndMetadataJ) }
        }

        def seekToBeginning(partitions: Nes[TopicPartition]) = {
          val partitionsJ = partitions.asJava
          serialNonBlocking { consumer.seekToBeginning(partitionsJ) }
        }

        def seekToEnd(partitions: Nes[TopicPartition]) = {
          val partitionsJ = partitions.asJava
          serialNonBlocking { consumer.seekToEnd(partitionsJ) }
        }

        def position(partition: TopicPartition) = {
          position1(partition) { consumer.position }
        }

        def position(partition: TopicPartition, timeout: FiniteDuration) = {
          position1(partition) { consumer.position(_, timeout.asJava) }
        }

        def committed(partitions: Nes[TopicPartition]) = {
          committed1(partitions) { consumer.committed }
        }

        def committed(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
          committed1(partitions) { consumer.committed(_, timeout.asJava) }
        }

        def partitions(topic: Topic) = {
          partitions1 { consumer.partitionsFor(topic) }
        }

        def partitions(topic: Topic, timeout: FiniteDuration) = {
          partitions1 { consumer.partitionsFor(topic, timeout.asJava) }
        }

        val topics = topics1 { consumer.listTopics() }

        def topics(timeout: FiniteDuration) = {
          topics1 { consumer.listTopics(timeout.asJava) }
        }

        def pause(partitions: Nes[TopicPartition]) = {
          val partitionsJ = partitions.asJava
          serialNonBlocking { consumer.pause(partitionsJ) }
        }

        val paused = {
          for {
            result <- serialNonBlocking { consumer.paused() }
            result <- topicPartitionsSetF[F](result)
          } yield result
        }

        def resume(partitions: Nes[TopicPartition]) = {
          val partitionsJ = partitions.asJava
          serialNonBlocking { consumer.resume(partitionsJ) }
        }

        def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) = {
          offsetsForTimes1(timestampsToSearch) { consumer.offsetsForTimes }
        }

        def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration) = {
          val timeoutJ = timeout.asJava
          offsetsForTimes1(timestampsToSearch) { consumer.offsetsForTimes(_, timeoutJ) }
        }

        def beginningOffsets(partitions: Nes[TopicPartition]) = {
          offsetsOf(partitions) { consumer.beginningOffsets }
        }

        def beginningOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
          val timeoutJ = timeout.asJava
          offsetsOf(partitions) { consumer.beginningOffsets(_, timeoutJ) }
        }

        def endOffsets(partitions: Nes[TopicPartition]) = {
          offsetsOf(partitions) { consumer.endOffsets }
        }

        def endOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
          val timeoutJ = timeout.asJava
          offsetsOf(partitions) { consumer.endOffsets(_, timeoutJ) }
        }

        def groupMetadata = {
          serialNonBlocking { consumer.groupMetadata() }.map { _.asScala }
        }

        val wakeup = {
          serialBlocking { consumer.wakeup() }
        }
      }
    }
  }


  private sealed abstract class WithMetrics

  private sealed abstract class MapK

  implicit class ConsumerOps[F[_], K, V](val self: Consumer[F, K, V]) extends AnyVal {

    def withMetrics[E](
      metrics: ConsumerMetrics[F]
    )(implicit F: MonadError[F, E], measureDuration: MeasureDuration[F]): Consumer[F, K, V] = {

      implicit val monoidUnit = Applicative.monoid[F, Unit]

      val topics = for {
        topicPartitions <- self.assignment
      } yield for {
        topicPartition <- topicPartitions
      } yield {
        topicPartition.topic
      }

      def call[A](name: String, topics: Iterable[Topic])(fa: F[A]): F[A] = {
        for {
          d <- MeasureDuration[F].start
          r <- fa.attempt
          d <- d
          _ <- topics.toList.foldMap { topic => metrics.call(name, topic, d, r.isRight) }
          r <- r.liftTo[F]
        } yield r
      }

      def call1[A](name: String)(f: F[A]): F[A] = {
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

      def rebalanceListener(listener: RebalanceListener[F]) = {

        def measure(name: String, partitions: Nes[TopicPartition]) = {
          partitions.foldMapM { metrics.rebalance(name, _) }
        }

        new WithMetrics with RebalanceListener[F] {

          def onPartitionsAssigned(partitions: Nes[TopicPartition]) = {
            for {
              _ <- measure("assigned", partitions)
              a <- listener.onPartitionsAssigned(partitions)
            } yield a
          }

          def onPartitionsRevoked(partitions: Nes[TopicPartition]) = {
            for {
              _ <- measure("revoked", partitions)
              a <- listener.onPartitionsRevoked(partitions)
            } yield a
          }

          def onPartitionsLost(partitions: Nes[TopicPartition]) = {
            for {
              _ <- measure("lost", partitions)
              a <- listener.onPartitionsLost(partitions)
            } yield a
          }
        }
      }

      new WithMetrics with Consumer[F, K, V] {

        def assign(partitions: Nes[TopicPartition]) = {
          val topics = partitions.map(_.topic).toList.toSet
          for {
            _ <- count("assign", topics)
            r <- self.assign(partitions)
          } yield r
        }

        val assignment = self.assignment

        def subscribe(topics: Nes[Topic], listener: RebalanceListener1[F]) = {
          // TODO RebalanceListener1 add metrics - https://github.com/evolution-gaming/skafka/issues/124
          for {
            _ <- count("subscribe", topics.toList)
            r <- self.subscribe(topics, listener)
          } yield r
        }

        def subscribe(topics: Nes[Topic]) = {
          for {
            _ <- count("subscribe", topics.toList)
            r <- self.subscribe(topics)
          } yield r
        }

        def subscribe(pattern: Pattern, listener: RebalanceListener1[F]) = {
          // TODO RebalanceListener1 add metrics - https://github.com/evolution-gaming/skafka/issues/124
          for {
            _ <- count("subscribe", List("pattern"))
            r <- self.subscribe(pattern, listener)
          } yield r
        }

        def subscribe(pattern: Pattern) = {
          for {
            _ <- count("subscribe", List("pattern"))
            r <- self.subscribe(pattern)
          } yield r
        }

        def subscribe(topics: Nes[Topic], listener: Option[RebalanceListener[F]]) = {
          val listener1 = listener.map(rebalanceListener)
          for {
            _ <- count("subscribe", topics.toList)
            r <- self.subscribe(topics, listener1)
          } yield r
        }

        def subscribe(pattern: Pattern, listener: Option[RebalanceListener[F]]) = {
          val listener1 = listener.map(rebalanceListener)
          for {
            _ <- count("subscribe", List("pattern"))
            r <- self.subscribe(pattern, listener1)
          } yield r
        }

        val subscription = self.subscription

        val unsubscribe = {
          call1("unsubscribe") { self.unsubscribe }
        }

        def poll(timeout: FiniteDuration) =
          for {
            records <- call1("poll") { self.poll(timeout) }
            topics   = records.values.values.flatMap(_.toList).groupBy(_.topic)
            _ <- topics.toList.traverse {
              case (topic, topicRecords) =>
                val bytes = topicRecords.flatMap(_.value).map(_.serializedSize).sum
                metrics.poll(topic, bytes = bytes, records = topicRecords.size)
            }
          } yield records

        val commit = {
          call1("commit") { self.commit }
        }

        def commit(timeout: FiniteDuration) = {
          call1("commit") { self.commit(timeout) }
        }

        def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]) = {
          val topics = offsets
            .keys
            .toList
            .map { _.topic }
          call("commit", topics) { self.commit(offsets) }
        }

        def commit(offsets: Nem[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = {
          val topics = offsets
            .keys
            .toList
            .map(_.topic)
          call("commit", topics) { self.commit(offsets, timeout) }
        }

        val commitLater = call1("commit_later") {
          self.commitLater
        }

        def commitLater(offsets: Nem[TopicPartition, OffsetAndMetadata]) = {
          val topics = offsets
            .keys
            .toList
            .map(_.topic)
          call("commit_later", topics) { self.commitLater(offsets) }
        }

        def seek(partition: TopicPartition, offset: Offset) = {
          for {
            _ <- count("seek", List(partition.topic))
            r <- self.seek(partition, offset)
          } yield r
        }

        def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) = {
          for {
            _ <- count("seek", List(partition.topic))
            r <- self.seek(partition, offsetAndMetadata)
          } yield r
        }

        def seekToBeginning(partitions: Nes[TopicPartition]) = {
          val topics = partitions.map(_.topic).toList
          for {
            _ <- count("seek_to_beginning", topics)
            r <- self.seekToBeginning(partitions)
          } yield r
        }

        def seekToEnd(partitions: Nes[TopicPartition]) = {
          val topics = partitions.map(_.topic).toList
          for {
            _ <- count("seek_to_end", topics)
            r <- self.seekToEnd(partitions)
          } yield r
        }

        def position(partition: TopicPartition) = {
          for {
            _ <- count("position", List(partition.topic))
            r <- self.position(partition)
          } yield r
        }

        def position(partition: TopicPartition, timeout: FiniteDuration) = {
          for {
            _ <- count("position", List(partition.topic))
            r <- self.position(partition, timeout)
          } yield r
        }

        def committed(partitions: Nes[TopicPartition]) = {
          def topics = partitions
            .toList
            .map { _.topic }
            .distinct
          for {
            _ <- count("committed", topics)
            r <- self.committed(partitions)
          } yield r
        }

        def committed(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
          def topics = partitions
            .toList
            .map { _.topic }
            .distinct
          for {
            _ <- count("committed", topics)
            r <- self.committed(partitions, timeout)
          } yield r
        }

        def partitions(topic: Topic) = {
          for {
            _ <- count("partitions", List(topic))
            r <- self.partitions(topic)
          } yield r
        }

        def partitions(topic: Topic, timeout: FiniteDuration) = {
          for {
            _ <- count("partitions", List(topic))
            r <- self.partitions(topic, timeout)
          } yield r
        }

        val topics = {
          for {
            d <- MeasureDuration[F].start
            r <- self.topics.attempt
            d <- d
            _ <- metrics.topics(d)
            r <- r.liftTo[F]
          } yield r
        }

        def topics(timeout: FiniteDuration) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.topics(timeout).attempt
            d <- d
            _ <- metrics.topics(d)
            r <- r.liftTo[F]
          } yield r
        }

        def pause(partitions: Nes[TopicPartition]) = {
          val topics = partitions.map(_.topic).toList
          for {
            _ <- count("pause", topics)
            r <- self.pause(partitions)
          } yield r
        }

        val paused = self.paused

        def resume(partitions: Nes[TopicPartition]) = {
          val topics = partitions.map(_.topic).toList
          for {
            _ <- count("resume", topics)
            r <- self.resume(partitions)
          } yield r
        }

        def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) = {
          val topics = timestampsToSearch.keySet.map(_.topic)
          call("offsets_for_times", topics) { self.offsetsForTimes(timestampsToSearch) }
        }

        def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration) = {
          val topics = timestampsToSearch.keySet.map(_.topic)
          call("offsets_for_times", topics) { self.offsetsForTimes(timestampsToSearch, timeout) }
        }

        def beginningOffsets(partitions: Nes[TopicPartition]) = {
          val topics = partitions.map(_.topic).toList
          call("beginning_offsets", topics) { self.beginningOffsets(partitions) }
        }

        def beginningOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
          val topics = partitions.map(_.topic).toList
          call("beginning_offsets", topics) { self.beginningOffsets(partitions, timeout) }
        }

        def endOffsets(partitions: Nes[TopicPartition]) = {
          val topics = partitions.map(_.topic).toList
          call("end_offsets", topics) { self.endOffsets(partitions) }
        }

        def endOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
          val topics = partitions.map(_.topic).toList
          call("end_offsets", topics) { self.endOffsets(partitions, timeout) }
        }

        def groupMetadata = {
          call1("group_metadata") { self.groupMetadata }
        }

        val wakeup = {
          for {
            _ <- count1("wakeup")
            r <- self.wakeup
          } yield r
        }
      }
    }

    def withLogging(log: Log[F])(implicit F: Monad[F], measureDuration: MeasureDuration[F]): Consumer[F, K, V] = {
      ConsumerLogging(log, self)
    }

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): Consumer[G, K, V] = new MapK with Consumer[G, K, V] {

      def assign(partitions: Nes[TopicPartition]) = fg(self.assign(partitions))

      def assignment = fg(self.assignment)

      def subscribe(topics: Nes[Topic], listener: RebalanceListener1[G]) = {
        val listener1 = listener.mapK(gf)
        fg(self.subscribe(topics, listener1))
      }

      def subscribe(topics: Nes[Topic]) = {
        fg(self.subscribe(topics))
      }

      def subscribe(pattern: Pattern, listener: RebalanceListener1[G]) = {
        val listener1 = listener.mapK(gf)
        fg(self.subscribe(pattern, listener1))
      }

      def subscribe(pattern: Pattern) = {
        fg(self.subscribe(pattern))
      }

      def subscribe(topics: Nes[Topic], listener: Option[RebalanceListener[G]]) = {
        val listener1 = listener.map(_.mapK(gf))
        fg(self.subscribe(topics, listener1))
      }

      def subscribe(pattern: Pattern, listener: Option[RebalanceListener[G]]) = {
        val listener1 = listener.map(_.mapK(gf))
        fg(self.subscribe(pattern, listener1))
      }

      def subscription = fg(self.subscription)

      def unsubscribe = fg(self.unsubscribe)

      def poll(timeout: FiniteDuration) = fg(self.poll(timeout))

      def commit = fg(self.commit)

      def commit(timeout: FiniteDuration) = fg(self.commit(timeout))

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]) = fg(self.commit(offsets))

      def commit(offsets: Nem[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) = fg(
        self.commit(offsets, timeout)
      )

      def commitLater = fg(self.commitLater)

      def commitLater(offsets: Nem[TopicPartition, OffsetAndMetadata]) = fg(self.commitLater(offsets))

      def seek(partition: TopicPartition, offset: Offset) = fg(self.seek(partition, offset))

      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) = fg(
        self.seek(partition, offsetAndMetadata)
      )

      def seekToBeginning(partitions: Nes[TopicPartition]) = fg(self.seekToBeginning(partitions))

      def seekToEnd(partitions: Nes[TopicPartition]) = fg(self.seekToEnd(partitions))

      def position(partition: TopicPartition) = fg(self.position(partition))

      def position(partition: TopicPartition, timeout: FiniteDuration) = fg(self.position(partition, timeout))

      def committed(partitions: Nes[TopicPartition]) = fg(self.committed(partitions))

      def committed(partitions: Nes[TopicPartition], timeout: FiniteDuration) = fg(self.committed(partitions, timeout))

      def partitions(topic: Topic) = fg(self.partitions(topic))

      def partitions(topic: Topic, timeout: FiniteDuration) = fg(self.partitions(topic, timeout))

      def topics = fg(self.topics)

      def topics(timeout: FiniteDuration) = fg(self.topics(timeout))

      def pause(partitions: Nes[TopicPartition]) = fg(self.pause(partitions))

      def paused = fg(self.paused)

      def resume(partitions: Nes[TopicPartition]) = fg(self.resume(partitions))

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) = {
        fg(self.offsetsForTimes(timestampsToSearch))
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration) = {
        fg(self.offsetsForTimes(timestampsToSearch, timeout))
      }

      def beginningOffsets(partitions: Nes[TopicPartition]) = fg(self.beginningOffsets(partitions))

      def beginningOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
        fg(self.beginningOffsets(partitions, timeout))
      }

      def endOffsets(partitions: Nes[TopicPartition]) = {
        fg(self.endOffsets(partitions))
      }

      def endOffsets(partitions: Nes[TopicPartition], timeout: FiniteDuration) = {
        fg(self.endOffsets(partitions, timeout))
      }

      def groupMetadata = fg(self.groupMetadata)

      def wakeup = fg(self.wakeup)
    }
  }
}
