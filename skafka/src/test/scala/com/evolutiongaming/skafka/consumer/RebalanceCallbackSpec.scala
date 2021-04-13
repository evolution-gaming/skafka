package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.{Duration => DurationJ}
import java.util.{Collection => CollectionJ, Map => MapJ, Set => SetJ}

import cats.effect.IO
import com.evolutiongaming.skafka.consumer.DataPoints._
import com.evolutiongaming.skafka.consumer.RebalanceCallback._
import com.evolutiongaming.skafka.consumer.RebalanceCallbackSpec._
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import org.apache.kafka.clients.consumer.{Consumer => ConsumerJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.util.control.NoStackTrace
import scala.util.{Failure, Try}

class RebalanceCallbackSpec extends AnyFreeSpec with Matchers {

  "RebalanceCallback" - {

    "consumer unrelated methods do nothing with consumer" - {
      val consumer: ConsumerJ[_, _] =
        null // null to verify zero interactions with consumer, otherwise there would be an NPE

      "empty just returns Unit" in {
        tryRun(RebalanceCallback.empty, consumer) mustBe Try(())
      }

      "pure just returns containing value" in {
        tryRun(RebalanceCallback.pure("value"), consumer) mustBe Try("value")
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
            } else fail("wrong input params")
          }
          override def beginningOffsets(
            p: CollectionJ[TopicPartitionJ],
            timeout: DurationJ
          ): MapJ[TopicPartitionJ, LongJ] = {
            if (p == partitions.j && timeout == timeouts.j) {
              offsetsMap.j
            } else fail("wrong input params")
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
          _      <- RebalanceCallback.empty
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

      "flatMap is free from StackOverflowError" in {
        val stackOverflowErrorDepth = 1000000
        // check that deep enough recursion results in StackOverflowError with current JVM settings
        try {
          // trigger SOE with given stackOverflowErrorDepth
          triggerStackOverflowError(stackOverflowErrorDepth)
          fail(
            s"expected a StackOverflowError from $stackOverflowErrorDepth-deep recursion, consider increasing the depth in test"
          )
        } catch {
          case _: StackOverflowError => // stackOverflowErrorDepth has correct value
        }

        val rc = List
          .fill(stackOverflowErrorDepth)(RebalanceCallback.empty)
          .fold(RebalanceCallback.empty) { (agg, e) => agg.flatMap(_ => e) }

        tryRun(rc, null) mustBe Try(())
      }

      "cats traverse is working" in {
        @volatile var seekResult: List[String] = List.empty
        val consumer = new ExplodingConsumer {
          override def seek(partition: TopicPartitionJ, offset: Long): Unit = {
            seekResult = seekResult :+ partition.topic()
          }
        }

        import cats.implicits._
        val topics = List(1, 2, 3)
        val rc: RebalanceCallback[IO, Unit] = for {
          _ <- topics.traverse_(i => seek(TopicPartition(s"$i", Partition.min), Offset.min))
        } yield ()

        RebalanceCallback.run(rc, consumer) mustBe Try(())
        seekResult mustBe topics.map(_.toString)
      }

    }

  }
}

object RebalanceCallbackSpec {

  def tryRun[A](rc: RebalanceCallback[Try, A], consumer: ConsumerJ[_, _]): Try[Any] = {
    RebalanceCallback.run[Try, A](rc, consumer)
  }

  case object TestError extends NoStackTrace

  case object TestError2 extends NoStackTrace

  def triggerStackOverflowError(n: Int): Int = {
    if (n <= 0) n
    else n + triggerStackOverflowError(n - 1)
  }
}
