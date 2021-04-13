package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.{Duration => DurationJ}
import java.util.concurrent.atomic.AtomicReference
import java.util.{Collection => CollectionJ, Map => MapJ, Set => SetJ}

import cats.effect.IO
import com.evolutiongaming.skafka.consumer.DataPoints._
import com.evolutiongaming.skafka.consumer.RebalanceCallback._
import com.evolutiongaming.skafka.consumer.RebalanceCallbackSpec._
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.util.control.NoStackTrace
import scala.util.{Failure, Try}

class RebalanceCallbackSpec extends AnyFreeSpec with Matchers {

  "RebalanceCallback" - {

    "consumer unrelated methods do nothing with consumer" - {
      val consumer: RebalanceConsumerJ =
        null // null to verify zero interactions with consumer, otherwise there would be an NPE

      "empty just returns Unit" in {
        tryRun(RebalanceCallback.empty, consumer) mustBe Try(())
      }

      "pure just returns containing value" in {
        tryRun(RebalanceCallback.pure("value"), consumer) mustBe Try("value")
      }

      "lift just returns the result of lifted computation" in {
        tryRun(lift(Try("ok")), consumer) mustBe Try("ok")
        RebalanceCallback.run(lift(IO.delay("can" + " do" + " stuff")), consumer) mustBe Try("can do stuff")
      }

      "fromTry just returns the result - Success" in {
        tryRun(fromTry(Try("ok")), consumer) mustBe Try("ok")
      }

      "fromTry just returns the result - Failure" in {
        tryRun(fromTry(Try(throw TestError)), consumer) mustBe Failure(TestError)
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

      "flatMap" - {
        "result from consumer method is visible to lifted computation" in {
          val expected = partitions.s

          val consumer = new ExplodingConsumer {
            override def assignment(): SetJ[TopicPartitionJ] = partitions.j

            override def paused(): SetJ[TopicPartitionJ] = throw TestError2
          }

          val rcOk = for {
            _      <- RebalanceCallback.empty
            result <- assignment
            _       = lift(IO.raiseError[Unit](TestError)) // should have no effect
            _       = paused // throws TestError2 but should have no effect
            a      <- lift(IO.delay { result })
          } yield a

          RebalanceCallback.run(rcOk, consumer) mustBe Try(expected)
        }

        "error from lifted computation" in {
          val a: AtomicReference[String] = new AtomicReference("unchanged")
          val b: AtomicReference[String] = new AtomicReference("unchanged")

          val consumer = new ExplodingConsumer {
            override def paused(): SetJ[TopicPartitionJ] = throw TestError2
          }

          val rcError1 = for {
            _ <- lift(IO.delay {
              a.getAndUpdate(_ => "step-before-rcError1")
            })
            _ <- lift(IO.raiseError[Unit](TestError)) // should fail the execution
            _ <- paused // paused throws TestError2, should not overwrite first error from lift
            _ <- lift(IO.delay {
              b.getAndUpdate(_ => "this change should not happen")
            })
          } yield ()

          val Failure(error) = RebalanceCallback.run(rcError1, consumer)
          error mustBe TestError
          a.get() mustBe "step-before-rcError1"
          b.get() mustBe "unchanged"

        }

        "error from consumer method" in {
          val a: AtomicReference[String] = new AtomicReference("unchanged")
          val b: AtomicReference[String] = new AtomicReference("unchanged")

          val consumer = new ExplodingConsumer {
            override def paused(): SetJ[TopicPartitionJ] = throw TestError2
          }

          val rcError2 = for {
            _ <- lift(IO.delay {
              a.getAndUpdate(_ => "step-before-rcError2")
            })
            _ <- paused // throws TestError2
            _ <- lift(
              IO.raiseError[Unit](TestError)
            ) // execution is failed already, should not overwrite first error from paused
            _ <- lift(IO.delay {
              b.getAndUpdate(_ => "this change should not happen 2")
            })
          } yield ()

          val Failure(error2) = RebalanceCallback.run(rcError2, consumer)
          error2 mustBe TestError2
          a.get() mustBe "step-before-rcError2"
          b.get() mustBe "unchanged"
        }

        "correct execution order" in {
          val list: AtomicReference[List[String]] = new AtomicReference(List.empty)

          val rcOk = for {
            _ <- lift(IO.delay {
              list.getAndUpdate(_ :+ "one")
            })
            _ <- lift(IO.delay {
              list.getAndUpdate(_ :+ "two")
            })
            _ <- lift(IO.delay {
              list.getAndUpdate(_ :+ "3")
            })
          } yield ()

          RebalanceCallback.run(rcOk, null) mustBe Try(())
          list.get() mustBe List("one", "two", "3")
        }

        "free from StackOverflowError" in {
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

      }

      "cats traverse is working" in {
        val seekResult: AtomicReference[List[String]] = new AtomicReference(List.empty)
        val consumer = new ExplodingConsumer {
          override def seek(partition: TopicPartitionJ, offset: LongJ): Unit = {
            val _ = seekResult.getAndUpdate(_ :+ partition.topic())
          }
        }

        import cats.implicits._
        val topics = List(1, 2, 3)
        val rc: RebalanceCallback[IO, Unit] = for {
          a <- topics.foldMapM(i => seek(TopicPartition(s"$i", Partition.min), Offset.min))
        } yield a

        RebalanceCallback.run(rc, consumer) mustBe Try(())
        seekResult.get() mustBe topics.map(_.toString)
      }

    }

  }
}

object RebalanceCallbackSpec {

  def tryRun[A](rc: RebalanceCallback[Try, A], consumer: RebalanceConsumerJ): Try[Any] = {
    RebalanceCallback.run[Try, A](rc, consumer)
  }

  case object TestError extends NoStackTrace

  case object TestError2 extends NoStackTrace

  def triggerStackOverflowError(n: Int): Int = {
    if (n <= 0) n
    else n + triggerStackOverflowError(n - 1)
  }
}
