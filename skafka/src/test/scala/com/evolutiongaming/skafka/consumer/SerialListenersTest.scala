package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util.{Collection => CollectionJ, List => ListJ, Map => MapJ, Set => SetJ}
import cats.data.{NonEmptySet => Nes}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, IO, Sync}
import cats.effect.implicits._
import cats.implicits._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{Blocking, ToFuture, ToTry}
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.IOSuite._
import com.evolutiongaming.skafka.consumer.ConsumerJHelper._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import com.evolutiongaming.skafka.{Bytes, Offset, Partition, TopicPartition}
import org.apache.kafka.clients.consumer.{
  ConsumerRebalanceListener,
  OffsetCommitCallback,
  Consumer => ConsumerJ,
  ConsumerRecord => ConsumerRecordJ,
  ConsumerRecords => ConsumerRecordsJ,
  OffsetAndMetadata => OffsetAndMetadataJ,
  OffsetAndTimestamp => OffsetAndTimestampJ
}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition => TopicPartitionJ}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace

class SerialListenersTest extends AsyncFunSuite with Matchers {
  import SerialListenersTest._

  test("consumer.poll") {
    `consumer.poll`[IO].run()
  }

  test("consumer.poll error") {
    `consumer.poll error`[IO].run()
  }

  @nowarn("cat=deprecation")
  private def `consumer.poll`[F[_]: Concurrent: ToTry: ToFuture: Blocking] = {

    val result = for {
      actions   <- Actions.of[F].toResource
      consumerJ <- consumerJ[F, Bytes, Bytes](actions).toResource
      consumer  <- Consumer.fromConsumerJ(consumerJ.pure[F])
      assigned0 <- Deferred[F, Unit].toResource
      assigned1 <- Deferred[F, Unit].toResource
      revoked0  <- Deferred[F, Unit].toResource
      revoked1  <- Deferred[F, Unit].toResource
      lost0     <- Deferred[F, Unit].toResource
      lost1     <- Deferred[F, Unit].toResource
      listener = new RebalanceListener[F] {

        def onPartitionsAssigned(partitions: Nes[TopicPartition]) = {
          for {
            _ <- actions.add(Action.PartitionsAssignedEnter)
            _ <- assigned0.complete(())
            _ <- assigned1.get
            _ <- actions.add(Action.PartitionsAssignedExit)
          } yield {}
        }

        def onPartitionsRevoked(partitions: Nes[TopicPartition]) = {
          for {
            _ <- actions.add(Action.PartitionsRevokedEnter)
            _ <- revoked0.complete(())
            _ <- revoked1.get
            _ <- actions.add(Action.PartitionsRevokedExit)
          } yield {}
        }

        def onPartitionsLost(partitions: Nes[TopicPartition]) = {
          for {
            _ <- actions.add(Action.PartitionsLostEnter)
            _ <- lost0.complete(())
            _ <- lost1.get
            _ <- actions.add(Action.PartitionsLostExit)
          } yield {}
        }
      }
      poll = for {
        _ <- actions.add(Action.PollEnter)
        a <- consumer.poll(1.millis)
        _ <- actions.add(Action.PollExit)
      } yield a
      _     <- consumer.subscribe(Nes.of("topic"), listener.some).toResource
      fiber <- poll.start.toResource

      _ <- assigned0.get.toResource
      _ <- consumer.topics.toResource
      _ <- assigned1.complete(()).toResource

      _ <- revoked0.get.toResource
      _ <- consumer.topics.toResource
      _ <- revoked1.complete(()).toResource

      _ <- lost0.get.toResource
      _ <- consumer.topics.toResource
      _ <- lost1.complete(()).toResource

      _       <- fiber.join.toResource
      actions <- actions.get.toResource
      _ = actions shouldEqual List(
        Action.PollEnter,
        Action.PartitionsAssignedEnter,
        Action.Topics,
        Action.PartitionsAssignedExit,
        Action.PartitionsRevokedEnter,
        Action.Topics,
        Action.PartitionsRevokedExit,
        Action.PartitionsLostEnter,
        Action.Topics,
        Action.PartitionsLostExit,
        Action.PollExit
      )
    } yield {}

    result.use { _.pure[F] }
  }

  @nowarn("cat=deprecation")
  private def `consumer.poll error`[F[_]: Concurrent: ToTry: ToFuture: Blocking] = {

    val error: Throwable = new RuntimeException("error") with NoStackTrace

    val result = for {
      actions   <- Actions.of[F].toResource
      consumerJ <- consumerJ[F, Bytes, Bytes](actions).toResource
      consumer  <- Consumer.fromConsumerJ(consumerJ.pure[F])
      deferred0 <- Deferred[F, Unit].toResource
      deferred1 <- Deferred[F, Unit].toResource
      listener = new RebalanceListener[F] {

        def onPartitionsAssigned(partitions: Nes[TopicPartition]) = {
          for {
            _ <- actions.add(Action.PartitionsAssignedEnter)
            _ <- deferred0.complete(())
            _ <- deferred1.get
            _ <- error.raiseError[F, Unit]
          } yield {}
        }

        def onPartitionsRevoked(partitions: Nes[TopicPartition]) = {
          for {
            _ <- actions.add(Action.PartitionsRevokedEnter)
            _ <- actions.add(Action.PartitionsRevokedExit)
          } yield {}
        }

        def onPartitionsLost(partitions: Nes[TopicPartition]) = {
          for {
            _ <- actions.add(Action.PartitionsLostEnter)
            _ <- actions.add(Action.PartitionsLostExit)
          } yield {}
        }
      }
      poll = for {
        _ <- actions.add(Action.PollEnter)
        a <- consumer.poll(1.millis)
        _ <- actions.add(Action.PollExit)
      } yield a
      _       <- consumer.subscribe(Nes.of("topic"), listener.some).toResource
      fiber   <- poll.start.toResource
      _       <- deferred0.get.toResource
      _       <- consumer.topics.toResource
      _       <- deferred1.complete(()).toResource
      _       <- consumer.topics.toResource
      result  <- fiber.join.attempt.toResource
      _        = result shouldEqual error.asLeft
      actions <- actions.get.toResource
      _ = actions shouldEqual List(Action.PollEnter, Action.PartitionsAssignedEnter, Action.Topics, Action.Topics)
    } yield {}

    result.use { _.pure[F] }
  }
}

object SerialListenersTest {

  val topic = "topic"

  sealed trait Action

  object Action {
    case object PollEnter extends Action
    case object PollExit extends Action
    case object PartitionsAssignedEnter extends Action
    case object PartitionsAssignedExit extends Action
    case object PartitionsRevokedEnter extends Action
    case object PartitionsRevokedExit extends Action
    case object PartitionsLostEnter extends Action
    case object PartitionsLostExit extends Action
    case object Topics extends Action
  }

  trait Actions[F[_]] {

    def add(action: Action): F[Unit]

    def get: F[List[Action]]
  }

  object Actions {
    def of[F[_]: Sync]: F[Actions[F]] = {
      Ref[F]
        .of(List.empty[Action])
        .map { ref =>
          new Actions[F] {
            def add(action: Action) = ref.update { action :: _ }

            def get = ref.get.map { _.reverse }
          }
        }
    }
  }

  def consumerJ[F[_]: Sync: ToTry, K, V](actions: Actions[F]): F[ConsumerJ[K, V]] = {
    for {
      listenerRef <- Ref[F].of(none[ConsumerRebalanceListener])
    } yield {
      val consumer: ConsumerJ[K, V] = new ConsumerJ[K, V] {

        def assignment() = Set.empty[TopicPartitionJ].asJava

        def subscription() = Set.empty[String].asJava

        def subscribe(topics: CollectionJ[String]) = {}

        def subscribe(topics: CollectionJ[String], listener: ConsumerRebalanceListener) = {
          listenerRef
            .set(listener.some)
            .toTry
            .get
        }

        def assign(partitions: CollectionJ[TopicPartitionJ]) = {}

        def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener) = {}

        def subscribe(pattern: Pattern) = {}

        def unsubscribe() = {}

        def poll(timeout: Long) = poll(Duration.ofMillis(timeout))

        def poll(timeout: Duration) = {
          val result = for {
            listener <- listenerRef.get
            _ <- listener.foldMapM { listener =>
              def partitions(value: Int) = {
                List(TopicPartition(topic, Partition.unsafe(value))).map { _.asJava }.asJava
              }
              for {
                _ <- Sync[F].delay { listener.onPartitionsAssigned(partitions(0)) }
                _ <- Sync[F].delay { listener.onPartitionsRevoked(partitions(1)) }
                _ <- Sync[F].delay { listener.onPartitionsLost(partitions(2)) }
              } yield {}
            }
          } yield {
            new ConsumerRecordsJ(Map.empty[TopicPartitionJ, ListJ[ConsumerRecordJ[K, V]]].asJava)
          }

          result
            .toTry
            .get
        }

        def commitSync() = {}

        def commitSync(timeout: Duration) = {}

        def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ]) = {}

        def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], timeout: Duration) = {}

        def commitAsync() = {}

        def commitAsync(callback: OffsetCommitCallback) = {}

        def commitAsync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], callback: OffsetCommitCallback) = {}

        def seek(partition: TopicPartitionJ, offset: Long) = {}

        def seek(partition: TopicPartitionJ, offsetAndMetadata: OffsetAndMetadataJ) = {}

        def seekToBeginning(partitions: CollectionJ[TopicPartitionJ]) = {}

        def seekToEnd(partitions: CollectionJ[TopicPartitionJ]) = {}

        def position(partition: TopicPartitionJ) = Offset.min.value

        def position(partition: TopicPartitionJ, timeout: Duration) = Offset.min.value

        def committed(partition: TopicPartitionJ) = {
          new OffsetAndMetadataJ(Offset.min.value, "metadata")
        }

        def committed(partition: TopicPartitionJ, timeout: Duration) = {
          new OffsetAndMetadataJ(Offset.min.value, "metadata")
        }

        def committed(partitions: SetJ[TopicPartitionJ]) = {
          Map.empty[TopicPartitionJ, OffsetAndMetadataJ].asJava
        }

        def committed(partitions: SetJ[TopicPartitionJ], timeout: Duration) = {
          Map.empty[TopicPartitionJ, OffsetAndMetadataJ].asJava
        }

        def metrics() = Map.empty[MetricName, Metric].asJava

        def partitionsFor(topic: String) = List.empty[PartitionInfo].asJava

        def partitionsFor(topic: String, timeout: Duration) = List.empty[PartitionInfo].asJava

        def listTopics() = {
          actions
            .add(Action.Topics)
            .as(Map.empty[String, ListJ[PartitionInfo]].asJava)
            .toTry
            .get
        }

        def listTopics(timeout: Duration) = {
          Map.empty[String, ListJ[PartitionInfo]].asJava
        }

        def paused() = Set.empty[TopicPartitionJ].asJava

        def pause(partitions: CollectionJ[TopicPartitionJ]) = {}

        def resume(partitions: CollectionJ[TopicPartitionJ]) = {}

        def offsetsForTimes(timestampsToSearch: MapJ[TopicPartitionJ, LongJ]) = {
          Map.empty[TopicPartitionJ, OffsetAndTimestampJ].asJava
        }

        def offsetsForTimes(timestampsToSearch: MapJ[TopicPartitionJ, LongJ], timeout: Duration) = {
          Map.empty[TopicPartitionJ, OffsetAndTimestampJ].asJava
        }

        def beginningOffsets(partitions: CollectionJ[TopicPartitionJ]) = {
          Map.empty[TopicPartitionJ, LongJ].asJava
        }

        def beginningOffsets(partitions: CollectionJ[TopicPartitionJ], timeout: Duration) = {
          Map.empty[TopicPartitionJ, LongJ].asJava
        }

        def endOffsets(partitions: CollectionJ[TopicPartitionJ]) = {
          Map.empty[TopicPartitionJ, LongJ].asJava
        }

        def endOffsets(partitions: CollectionJ[TopicPartitionJ], timeout: Duration) = {
          Map.empty[TopicPartitionJ, LongJ].asJava
        }

        def groupMetadata() = ConsumerGroupMetadata.Empty.asJava

        def enforceRebalance() = {}

        def close() = {}

        def close(timeout: Long, unit: TimeUnit) = {}

        def close(timeout: Duration) = {}

        def wakeup() = {}
      }

      consumer.errorOnConcurrentAccess
    }
  }
}
