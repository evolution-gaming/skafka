package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.{Duration => DurationJ}
import java.util.concurrent.{TimeUnit => TimeUnitJ}
import java.util.regex.Pattern
import java.util.{Collection => CollectionJ, List => ListJ, Map => MapJ, Set => SetJ}

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.effect.IO
import com.evolutiongaming.skafka.consumer.RebalanceCallback._
import com.evolutiongaming.skafka.consumer.RebalanceCallbackSpec._
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{
  ConsumerRebalanceListener,
  OffsetAndMetadata,
  OffsetCommitCallback,
  Consumer => ConsumerJ
}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition => TopicPartitionJ}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import com.evolutiongaming.skafka.IOSuite._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}
import scala.util.control.NoStackTrace

class RebalanceCallbackSpec extends AnyFreeSpec with Matchers {

  "RebalanceCallback" - {
    "consumer unrelated methods do nothing with consumer" - {
      val consumer: ConsumerJ[_, _] =
        null // null to verify zero interactions with consumer, otherwise there would be an NPE

      "noOp just returns Unit" in {
        tryRun(noOp, consumer) mustBe Try(())
      }

      "lift just returns the result of lifted computation" in {
        tryRun(lift(Try("ok")), consumer) mustBe Try("ok")
      }
    }

    "consumer related methods delegating the call correctly" - {

      "assignment" in {
        val expected = partitions.s

        val consumer = new ExplodingConsumer {
          override def assignment(): SetJ[TopicPartitionJ] = partitions.j
        }

        tryRun(assignment, consumer) mustBe Try(expected)
      }

      "beginningOffsets" in {
        val input    = partitions.s
        val expected = offsetsMap.s

        val consumer = new ExplodingConsumer {
          override def beginningOffsets(p: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = {
            if (p == partitions.j) {
              offsetsMap.j
            } else ???
          }
          override def beginningOffsets(
            p: CollectionJ[TopicPartitionJ],
            timeout: DurationJ
          ): MapJ[TopicPartitionJ, LongJ] = {
            if (p == partitions.j && timeout == timeouts.j) {
              offsetsMap.j
            } else ???
          }
        }

        tryRun(beginningOffsets(input), consumer) mustBe Try(expected)
        tryRun(beginningOffsets(input, timeouts.s), consumer) mustBe Try(expected)
      }

    }

    "composability" - {
      "flatMap" in {
        val expected               = partitions.s
        var a: Set[TopicPartition] = Set.empty
        var b: String              = "unchanged"
        var c: String              = "unchanged"

        val consumer = new ExplodingConsumer {
          override def assignment(): SetJ[TopicPartitionJ] = partitions.j

          override def paused(): SetJ[TopicPartitionJ] = throw TestError2
        }

        val rcOk = for {
          _      <- noOp[IO]
          result <- assignment
          _       = lift(IO.raiseError[Unit](TestError)) // should have no effect
          _       = paused // throws TestError2 but should have no effect
          _ <- lift(IO.delay {
            a = result
          })
        } yield ()

        val rcError1 = for {
          _ <- lift(IO.delay {
            c = "rcError1"
          })
          _ <- lift(IO.raiseError[Unit](TestError)) // should fail the execution
          _ <- paused // paused throws TestError2, should not overwrite first error from lift
          _ <- lift(IO.delay {
            b = "this change should not happen"
          })
        } yield ()

        val rcError2 = for {
          _ <- lift(IO.delay {
            c = "rcError2"
          })
          _ <- paused // throws TestError2
          _ <- lift(
            IO.raiseError[Unit](TestError)
          ) // execution is failed already, should not overwrite first error from paused
          _ <- lift(IO.delay {
            b = "this change should not happen 2"
          })
        } yield ()

        val ok = RebalanceCallback.run(rcOk, consumer)
        ok mustBe Try(())
        a mustBe expected
        c mustBe "unchanged"

        val Failure(error) = RebalanceCallback.run(rcError1, consumer)
        error mustBe TestError
        b mustBe "unchanged"
        c mustBe "rcError1"

        val Failure(error2) = RebalanceCallback.run(rcError2, consumer)
        error2 mustBe TestError2
        b mustBe "unchanged"
        c mustBe "rcError2"
      }
      "flatMap correct execution order" in {
        var list: List[String] = List.empty

        val rcOk = for {
          _ <- lift(IO.delay {
            list = list :+ "one"
          })
          _ <- lift(IO.delay {
            list = list :+ "two"
          })
          _ <- lift(IO.delay {
            list = list :+ "3"
          })
        } yield ()

        RebalanceCallback.run(rcOk, null) mustBe Try(())
        list mustBe List("one", "two", "3")
      }
    }
  }
}

object RebalanceCallbackSpec {

  def tryRun[A](rc: RebalanceCallback[Try, A], consumer: ConsumerJ[_, _]): Try[A] = {
    RebalanceCallback.run[Try, A](rc, consumer)
  }

  final case class JavaScala[J, S](j: J, s: S)

  case object TestError extends NoStackTrace

  case object TestError2 extends NoStackTrace

  // - it is intentional to have all methods as `???` (throws NotImplementedError)
  // - it is used to verify the only expected interaction in corresponding tests
  //   by implementing the only expected method to be called in test
  class ExplodingConsumer extends ConsumerJ[String, String] {
    def assignment(): SetJ[TopicPartitionJ] = ???

    def subscription(): SetJ[String] = ???

    def subscribe(topics: CollectionJ[String]): Unit = ???

    def subscribe(topics: CollectionJ[String], callback: ConsumerRebalanceListener): Unit = ???

    def assign(partitions: CollectionJ[TopicPartitionJ]): Unit = ???

    def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): Unit = ???

    def subscribe(pattern: Pattern): Unit = ???

    def unsubscribe(): Unit = ???

    def poll(timeout: Long): consumer.ConsumerRecords[String, String] = ???

    def poll(timeout: DurationJ): consumer.ConsumerRecords[String, String] = ???

    def commitSync(): Unit = ???

    def commitSync(timeout: DurationJ): Unit = ???

    def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadata]): Unit = ???

    def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadata], timeout: DurationJ): Unit = ???

    def commitAsync(): Unit = ???

    def commitAsync(callback: OffsetCommitCallback): Unit = ???

    def commitAsync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadata], callback: OffsetCommitCallback): Unit = ???

    def seek(partition: TopicPartitionJ, offset: Long): Unit = ???

    def seek(partition: TopicPartitionJ, offsetAndMetadata: OffsetAndMetadata): Unit = ???

    def seekToBeginning(partitions: CollectionJ[TopicPartitionJ]): Unit = ???

    def seekToEnd(partitions: CollectionJ[TopicPartitionJ]): Unit = ???

    def position(partition: TopicPartitionJ): Long = ???

    def position(partition: TopicPartitionJ, timeout: DurationJ): Long = ???

    def committed(partition: TopicPartitionJ): OffsetAndMetadata = ???

    def committed(partition: TopicPartitionJ, timeout: DurationJ): OffsetAndMetadata = ???

    def committed(partitions: SetJ[TopicPartitionJ]): MapJ[TopicPartitionJ, OffsetAndMetadata] = ???

    def committed(
      partitions: SetJ[TopicPartitionJ],
      timeout: DurationJ
    ): MapJ[TopicPartitionJ, OffsetAndMetadata] = ???

    def metrics(): MapJ[MetricName, _ <: Metric] = ???

    def partitionsFor(topic: String): ListJ[PartitionInfo] = ???

    def partitionsFor(topic: String, timeout: DurationJ): ListJ[PartitionInfo] = ???

    def listTopics(): MapJ[String, ListJ[PartitionInfo]] = ???

    def listTopics(timeout: DurationJ): MapJ[String, ListJ[PartitionInfo]] = ???

    def paused(): SetJ[TopicPartitionJ] = ???

    def pause(partitions: CollectionJ[TopicPartitionJ]): Unit = ???

    def resume(partitions: CollectionJ[TopicPartitionJ]): Unit = ???

    def offsetsForTimes(
      timestampsToSearch: MapJ[TopicPartitionJ, LongJ]
    ): MapJ[TopicPartitionJ, consumer.OffsetAndTimestamp] = ???

    def offsetsForTimes(
      timestampsToSearch: MapJ[TopicPartitionJ, LongJ],
      timeout: DurationJ
    ): MapJ[TopicPartitionJ, consumer.OffsetAndTimestamp] = ???

    def beginningOffsets(partitions: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = ???

    def beginningOffsets(
      partitions: CollectionJ[TopicPartitionJ],
      timeout: DurationJ
    ): MapJ[TopicPartitionJ, LongJ] = ???

    def endOffsets(partitions: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = ???

    def endOffsets(
      partitions: CollectionJ[TopicPartitionJ],
      timeout: DurationJ
    ): MapJ[TopicPartitionJ, LongJ] = ???

    def groupMetadata(): consumer.ConsumerGroupMetadata = ???

    def close(): Unit = ???

    def close(timeout: Long, unit: TimeUnitJ): Unit = ???

    def close(timeout: DurationJ): Unit = ???

    def wakeup(): Unit = ???
  }

  val partitions: JavaScala[SetJ[TopicPartitionJ], Nes[TopicPartition]] = JavaScala(
    Set(
      new TopicPartitionJ("topic", 3),
      new TopicPartitionJ("topicc", 42)
    ).asJava,
    Nes.of(
      TopicPartition("topic", Partition.unsafe(3)),
      TopicPartition("topicc", Partition.unsafe(42))
    )
  )

  val offsetsMap: JavaScala[MapJ[TopicPartitionJ, LongJ], Nem[TopicPartition, Offset]] = JavaScala(
    Set(
      new TopicPartitionJ("topic", 3)   -> LongJ.valueOf(39L),
      new TopicPartitionJ("topicc", 42) -> LongJ.valueOf(71L)
    ).toMap.asJava,
    Nes
      .of(
        TopicPartition("topic", Partition.unsafe(3))   -> Offset.unsafe(39),
        TopicPartition("topicc", Partition.unsafe(42)) -> Offset.unsafe(71)
      )
      .toNonEmptyList
      .toNem
  )

  val timeouts: JavaScala[DurationJ, FiniteDuration] = JavaScala(
    DurationJ.ofSeconds(7),
    7.seconds
  )
}
