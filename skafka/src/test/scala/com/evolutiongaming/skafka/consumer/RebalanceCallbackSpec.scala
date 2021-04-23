package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.{Duration => DurationJ}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.{Collection => CollectionJ, List => ListJ, Map => MapJ, Set => SetJ}

import cats.arrow.FunctionK
import cats.effect.IO
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{ToFuture, ToTry}
import com.evolutiongaming.skafka.Converters.{SkafkaOffsetAndMetadataOpsConverters, TopicPartitionOps}
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.DataPoints._
import com.evolutiongaming.skafka.consumer.RebalanceCallback._
import com.evolutiongaming.skafka.consumer.RebalanceCallbackSpec._
import org.apache.kafka.clients.consumer.{
  ConsumerGroupMetadata => ConsumerGroupMetadataJ,
  OffsetAndMetadata => OffsetAndMetadataJ,
  OffsetAndTimestamp => OffsetAndTimestampJ
}
import org.apache.kafka.common.{PartitionInfo => PartitionInfoJ, TopicPartition => TopicPartitionJ}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Try}

class RebalanceCallbackSpec extends AnyFreeSpec with Matchers {

  "RebalanceCallback" - {

    "consumer unrelated methods do nothing with consumer" - {
      val consumer = new ExplodingConsumer

      "empty just returns Unit" in {
        tryRun(RebalanceCallback.empty, consumer) mustBe Try(())
        ioTests(_.empty, (), consumer)
      }

      "pure just returns containing value" in {
        val expected = "value"

        tryRun(RebalanceCallback.pure(expected), consumer) mustBe Try(expected)
        ioTests(_.pure(expected), expected, consumer)
      }

      "lift just returns the result of lifted computation" in {
        val expected = "can do stuff"

        tryRun(lift(Try(expected)), consumer) mustBe Try(expected)
        ioTests(_.lift(IO.delay("can" + " do" + " stuff")), expected, consumer)
      }

      "fromTry just returns the result - Success" in {
        val expected = "ok"

        tryRun(fromTry(Try(expected)), consumer) mustBe Try(expected)
        ioTests(_.fromTry(Try(expected)), expected, consumer)
      }

      "fromTry just returns the result - Failure" in {
        val input = Try(throw TestError)

        tryRun(fromTry(input), consumer) mustBe Failure(TestError)
        ioErrorTests(RebalanceCallback.api[IO].fromTry(input), TestError, consumer)
      }
    }

    "consumer related methods delegating the call correctly" - {

      "assignment" in {
        val output = partitions.s

        val consumer = new ExplodingConsumer {
          override def assignment(): SetJ[TopicPartitionJ] = partitions.j
        }

        tryRun(assignment, consumer) mustBe Try(output)
        ioTests(_.assignment, output, consumer)
      }

      "beginningOffsets" in {
        val input  = partitions
        val output = offsetsMap
        val consumer = new ExplodingConsumer {
          override def beginningOffsets(p: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = {
            p mustBe input.j
            output.j
          }

          override def beginningOffsets(
            p: CollectionJ[TopicPartitionJ],
            timeout: DurationJ
          ): MapJ[TopicPartitionJ, LongJ] = {
            p mustBe input.j
            timeout mustBe timeouts.j
            output.j
          }
        }

        tryRun(beginningOffsets(input.s), consumer) mustBe Try(output.s)
        tryRun(beginningOffsets(input.s, timeouts.s), consumer) mustBe Try(output.s)
        ioTests(_.beginningOffsets(input.s), output.s, consumer)
        ioTests(_.beginningOffsets(input.s, timeouts.s), output.s, consumer)
      }

      "commit" in {
        val input = offsetsAndMetadataMap
        val consumer = new ExplodingConsumer {
          override def commitSync(): Unit = ()

          override def commitSync(timeout: DurationJ): Unit = {
            val _ = timeout mustBe timeouts.j
          }

          override def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ]): Unit = {
            val _ = offsets mustBe input.j
          }

          override def commitSync(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], timeout: DurationJ): Unit = {
            val _ = timeout mustBe timeouts.j
            offsets mustBe input.j
            ()
          }
        }

        tryRun(commit, consumer) mustBe Try(())
        tryRun(commit(timeouts.s), consumer) mustBe Try(())
        tryRun(commit(input.s), consumer) mustBe Try(())
        tryRun(commit(input.s, timeouts.s), consumer) mustBe Try(())
        ioTests(_.commit, (), consumer)
        ioTests(_.commit(timeouts.s), (), consumer)
        ioTests(_.commit(input.s), (), consumer)
        ioTests(_.commit(input.s, timeouts.s), (), consumer)
      }

      "committed" in {
        val input  = partitions
        val output = offsetsAndMetadataMap
        val consumer = new ExplodingConsumer {
          override def committed(p: SetJ[TopicPartitionJ]): MapJ[TopicPartitionJ, OffsetAndMetadataJ] = {
            p mustBe input.j
            output.j
          }

          override def committed(
            p: SetJ[TopicPartitionJ],
            timeout: DurationJ
          ): MapJ[TopicPartitionJ, OffsetAndMetadataJ] = {
            timeout mustBe timeouts.j
            p mustBe input.j
            output.j
          }
        }

        tryRun(committed(input.s), consumer) mustBe Try(output.s)
        tryRun(committed(input.s, timeouts.s), consumer) mustBe Try(output.s)
        ioTests(_.committed(input.s), output.s, consumer)
        ioTests(_.committed(input.s, timeouts.s), output.s, consumer)
      }

      "endOffsets" in {
        val input  = partitions
        val output = offsetsMap
        val consumer = new ExplodingConsumer {
          override def endOffsets(p: CollectionJ[TopicPartitionJ]): MapJ[TopicPartitionJ, LongJ] = {
            p mustBe input.j
            output.j
          }

          override def endOffsets(
            p: CollectionJ[TopicPartitionJ],
            timeout: DurationJ
          ): MapJ[TopicPartitionJ, LongJ] = {
            timeout mustBe timeouts.j
            p mustBe input.j
            output.j
          }
        }

        tryRun(endOffsets(input.s), consumer) mustBe Try(output.s)
        tryRun(endOffsets(input.s, timeouts.s), consumer) mustBe Try(output.s)
        ioTests(_.endOffsets(input.s), output.s, consumer)
        ioTests(_.endOffsets(input.s, timeouts.s), output.s, consumer)
      }

      "groupMetadata" in {
        val output = consumerGroupMetadata

        val consumer = new ExplodingConsumer {
          override def groupMetadata(): ConsumerGroupMetadataJ = output.j
        }

        tryRun(groupMetadata, consumer) mustBe Try(output.s)
        ioTests(_.groupMetadata, output.s, consumer)
      }

      "topics" in {
        val output = partitionInfoMap

        val consumer = new ExplodingConsumer {
          override def listTopics(): MapJ[Topic, ListJ[PartitionInfoJ]] = output.j
          override def listTopics(timeout: DurationJ): MapJ[Topic, ListJ[PartitionInfoJ]] = {
            timeout mustBe timeouts.j
            output.j
          }
        }

        tryRun(topics, consumer) mustBe Try(output.s)
        tryRun(topics(timeouts.s), consumer) mustBe Try(output.s)
        ioTests(_.topics, output.s, consumer)
        ioTests(_.topics(timeouts.s), output.s, consumer)
      }

      "offsetsForTimes" in {
        val input  = timeStampsToSearchMap
        val output = offsetsForTimesResponse

        val consumer = new ExplodingConsumer {
          override def offsetsForTimes(
            timestampsToSearch: MapJ[TopicPartitionJ, LongJ]
          ): MapJ[TopicPartitionJ, OffsetAndTimestampJ] = {
            timestampsToSearch mustBe input.j
            output.j
          }
          override def offsetsForTimes(
            timestampsToSearch: MapJ[TopicPartitionJ, LongJ],
            timeout: DurationJ
          ): MapJ[TopicPartitionJ, OffsetAndTimestampJ] = {
            timeout mustBe timeouts.j
            timestampsToSearch mustBe input.j
            output.j
          }
        }

        tryRun(offsetsForTimes(input.s), consumer) mustBe Try(output.s)
        tryRun(offsetsForTimes(input.s, timeouts.s), consumer) mustBe Try(output.s)
        ioTests(_.offsetsForTimes(input.s), output.s, consumer)
        ioTests(_.offsetsForTimes(input.s, timeouts.s), output.s, consumer)
      }

      "partitionsFor" in {
        val input  = partitionInfo.s.topic
        val output = partitionInfoList

        val consumer = new ExplodingConsumer {
          override def partitionsFor(topic: String): ListJ[PartitionInfoJ] = {
            topic mustBe input
            output.j
          }
          override def partitionsFor(topic: String, timeout: DurationJ): ListJ[PartitionInfoJ] = {
            timeout mustBe timeouts.j
            topic mustBe input
            output.j
          }
        }

        tryRun(partitionsFor(input), consumer) mustBe Try(output.s)
        tryRun(partitionsFor(input, timeouts.s), consumer) mustBe Try(output.s)
        ioTests(_.partitionsFor(input), output.s, consumer)
        ioTests(_.partitionsFor(input, timeouts.s), output.s, consumer)
      }

      "paused" in {
        val output = partitions

        val consumer = new ExplodingConsumer {
          override def paused(): SetJ[TopicPartitionJ] = output.j
        }

        tryRun(paused, consumer) mustBe Try(output.s)
        ioTests(_.paused, output.s, consumer)
      }

      "position" in {
        val input  = partitions.s.head
        val output = Offset.unsafe(323)

        val consumer = new ExplodingConsumer {
          override def position(p: TopicPartitionJ): LongJ = {
            p mustBe input.asJava
            output.value
          }
          override def position(p: TopicPartitionJ, timeout: DurationJ): LongJ = {
            timeout mustBe timeouts.j
            p mustBe input.asJava
            output.value
          }
        }

        tryRun(position(input), consumer) mustBe Try(output)
        tryRun(position(input, timeouts.s), consumer) mustBe Try(output)
        ioTests(_.position(input), output, consumer)
        ioTests(_.position(input, timeouts.s), output, consumer)
      }

      "seek" in {
        val input                  = partitions.s.head
        val inputOffset            = Offset.unsafe(423)
        val inputOffsetAndMetadata = OffsetAndMetadata(inputOffset)
        val output                 = ()

        val consumer = new ExplodingConsumer {
          override def seek(p: TopicPartitionJ, offset: LongJ): Unit = {
            val _ = p mustBe input.asJava
          }

          override def seek(p: TopicPartitionJ, offsetAndMetadataJ: OffsetAndMetadataJ): Unit = {
            p mustBe input.asJava
            val _ = offsetAndMetadataJ mustBe inputOffsetAndMetadata.asJava
          }

        }

        tryRun(seek(input, inputOffset), consumer) mustBe Try(output)
        tryRun(seek(input, inputOffsetAndMetadata), consumer) mustBe Try(output)
        ioTests(_.seek(input, inputOffset), output, consumer)
        ioTests(_.seek(input, inputOffsetAndMetadata), output, consumer)
      }

      "seekToBeginning" in {
        val input  = partitions
        val output = ()

        val consumer = new ExplodingConsumer {
          override def seekToBeginning(p: CollectionJ[TopicPartitionJ]): Unit = {
            val _ = p mustBe input.j
          }
        }

        tryRun(seekToBeginning(input.s), consumer) mustBe Try(output)
        ioTests(_.seekToBeginning(input.s), output, consumer)
      }

      "seekToEnd" in {
        val input  = partitions
        val output = ()

        val consumer = new ExplodingConsumer {
          override def seekToEnd(p: CollectionJ[TopicPartitionJ]): Unit = {
            val _ = p mustBe input.j
          }
        }

        tryRun(seekToEnd(input.s), consumer) mustBe Try(output)
        ioTests(_.seekToEnd(input.s), output, consumer)
      }

      "subscription" in {
        val output = partitions.s.map(_.topic).toSortedSet

        val consumer = new ExplodingConsumer {
          override def subscription(): SetJ[String] = output.asJava
        }

        tryRun(subscription, consumer) mustBe Try(output)
        ioTests(_.subscription, output, consumer)
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

          rcOk.run(consumer) mustBe Try(expected)
          ioTests(_ => rcOk, expected, consumer)
        }

        "error from lifted computation" in {
          val a: AtomicReference[String] = new AtomicReference("unchanged")
          val b: AtomicReference[String] = new AtomicReference("unchanged")

          val consumer = new ExplodingConsumer {
            override def paused(): SetJ[TopicPartitionJ] = throw TestError2
          }

          val rcError1 = for {
            _ <- lift(IO.delay {
              a.set("step-before-rcError1")
            })
            _ <- lift(IO.raiseError[Unit](TestError)) // should fail the execution
            _ <- paused // paused throws TestError2, should not overwrite first error from lift
            _ <- lift(IO.delay {
              b.set("this change should not happen")
            })
          } yield ()

          val reset = () => {
            a.set("unchanged")
            b.set("unchanged")
          }
          val verifyResults = () => {
            a.get() mustBe "step-before-rcError1"
            b.get() mustBe "unchanged"
            ()
          }

          ioErrorTests(rcError1, TestError, consumer, reset, verifyResults)
        }

        "error from consumer method" in {
          val a: AtomicReference[String] = new AtomicReference("unchanged")
          val b: AtomicReference[String] = new AtomicReference("unchanged")

          val consumer = new ExplodingConsumer {
            override def paused(): SetJ[TopicPartitionJ] = throw TestError2
          }

          val rcError2 = for {
            _ <- lift(IO.delay {
              a.set("step-before-rcError2")
            })
            _ <- paused // throws TestError2
            _ <- lift(
              IO.raiseError[Unit](TestError)
            ) // execution is failed already, should not overwrite first error from paused
            _ <- lift(IO.delay {
              b.set("this change should not happen 2")
            })
          } yield ()

          val reset = () => {
            a.set("unchanged")
            b.set("unchanged")
          }
          val verifyResults = () => {
            a.get() mustBe "step-before-rcError2"
            b.get() mustBe "unchanged"
            ()
          }

          ioErrorTests(rcError2, TestError2, consumer, reset, verifyResults)
        }

        "correct and complete order of execution" in {
          val list: AtomicReference[List[String]] = new AtomicReference(List.empty)
          val expected                            = List("one", "two", "3")
          val consumer                            = new ExplodingConsumer

          val rc: RebalanceCallback[IO, Int] = for {
            two <- lift(IO.delay {
              list.getAndUpdate(_ :+ "one")
              "two"
            })
            three <- lift(IO.delay {
              list.getAndUpdate(_ :+ two)
              "3"
            })
            a <- lift(IO.delay {
              list.getAndUpdate(_ :+ three)
              42
            })
          } yield a

          val reset = () => {
            list.set(List.empty)
          }
          val verifyResults = () => {
            list.get() mustBe expected
            ()
          }

          ioTests(_ => rc, 42, consumer, reset, verifyResults)
        }

        "free from StackOverflowError" - {
          val stackOverflowErrorDepth = 1000000
          s"SOE is reproducible with a depth of $stackOverflowErrorDepth" in {
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
          }

          val consumer = new ExplodingConsumer {
            override def commitSync(): Unit = ()
          }

          "with IO effect type" in {
            val api = RebalanceCallback.api[IO]

            val rc: RebalanceCallback[IO, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.empty)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            val rc1: RebalanceCallback[IO, Int] = Vector
              .fill(stackOverflowErrorDepth)(api.lift(IO.delay(1)))
              .fold(api.lift(IO.delay(0))) { (agg, e) => agg.flatMap(acc => e.map(_ + acc)) }

            val rc2: RebalanceCallback[IO, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.commit)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            rc.toF(consumer).unsafeRunSync() mustBe ()
            rc1.toF(consumer).unsafeRunSync() mustBe stackOverflowErrorDepth
            rc2.toF(consumer).unsafeRunSync() mustBe ()
          }

          "with Try effect type" in {
            val api = RebalanceCallback

            val rc: RebalanceCallback[Nothing, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.empty)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            val rc1: RebalanceCallback[IO, Int] = Vector
              .fill(stackOverflowErrorDepth)(api.lift(IO.delay(1)))
              .fold(api.lift(IO.delay(0))) { (agg, e) => agg.flatMap(acc => e.map(_ + acc)) }

            val rc2: RebalanceCallback[Nothing, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.commit)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            tryRun(rc, consumer) mustBe Try(())
            rc1.run(consumer) mustBe Try(stackOverflowErrorDepth)
            tryRun(rc2, consumer) mustBe Try(())
          }

          "mapK with Try ~> Try" in {
            val api = RebalanceCallback

            val rc: RebalanceCallback[Nothing, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.empty)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            val rc1: RebalanceCallback[IO, Int] = Vector
              .fill(stackOverflowErrorDepth)(api.lift(IO.delay(1)))
              .fold(api.lift(IO.delay(0))) { (agg, e) => agg.flatMap(acc => e.map(_ + acc)) }

            val rc2: RebalanceCallback[Nothing, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.commit)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            tryRun(rc.mapK(FunctionK.id), consumer) mustBe Try(())
            rc1.mapK(FunctionK.id).run(consumer) mustBe Try(stackOverflowErrorDepth)
            tryRun(rc2.mapK(FunctionK.id), consumer) mustBe Try(())
          }

          "mapK with IO ~> Try" in {
            val api = RebalanceCallback.api[IO]

            val rc: RebalanceCallback[IO, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.empty)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            val rc1: RebalanceCallback[IO, Int] = Vector
              .fill(stackOverflowErrorDepth)(api.lift(IO.delay(1)))
              .fold(api.lift(IO.delay(0))) { (agg, e) => agg.flatMap(acc => e.map(_ + acc)) }

            val rc2: RebalanceCallback[IO, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.commit)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            rc.mapK(ToTry.functionK).run(consumer) mustBe Try(())
            rc1.mapK(ToTry.functionK).run(consumer) mustBe Try(stackOverflowErrorDepth)
            rc2.mapK(ToTry.functionK).run(consumer) mustBe Try(())
          }

          "mapK with IO ~> Future" in {
            val api = RebalanceCallback.api[IO]

            val rc: RebalanceCallback[IO, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.empty)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            val rc1: RebalanceCallback[IO, Int] = Vector
              .fill(stackOverflowErrorDepth)(api.lift(IO.delay(1)))
              .fold(api.lift(IO.delay(0))) { (agg, e) => agg.flatMap(acc => e.map(_ + acc)) }

            val rc2: RebalanceCallback[IO, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.commit)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            def futureToTry(timeout: FiniteDuration): ToTry[Future] = new ToTry[Future] {
              def apply[A](fa: Future[A]) = {
                Try { Await.result(fa, timeout) }
              }
            }
            implicit val defaultFutureToTry: ToTry[Future] = futureToTry(30.seconds)

            rc.mapK(ToFuture.functionK).run(consumer) mustBe Try(())
            rc1.mapK(ToTry.functionK).run(consumer) mustBe Try(stackOverflowErrorDepth)
            rc2.mapK(ToTry.functionK).run(consumer) mustBe Try(())
          }

          "mapK with IO ~> IO" in {
            val api = RebalanceCallback.api[IO]

            val rc: RebalanceCallback[IO, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.empty)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            val rc1: RebalanceCallback[IO, Int] = Vector
              .fill(stackOverflowErrorDepth)(api.lift(IO.delay(1)))
              .fold(api.lift(IO.delay(0))) { (agg, e) => agg.flatMap(acc => e.map(_ + acc)) }

            val rc2: RebalanceCallback[IO, Unit] = Vector
              .fill(stackOverflowErrorDepth)(api.commit)
              .fold(api.empty) { (agg, e) => agg.flatMap(_ => e) }

            rc.mapK(FunctionK.id).toF(consumer).unsafeRunSync() mustBe ()
            rc1.mapK(FunctionK.id).toF(consumer).unsafeRunSync() mustBe stackOverflowErrorDepth
            rc2.mapK(FunctionK.id).toF(consumer).unsafeRunSync() mustBe ()
          }
        }

      }

      "cats traverse is working for RebalanceCallback[Nothing, *]" in {
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

        rc.run(consumer) mustBe Try(())
        seekResult.get() mustBe topics.map(_.toString)
      }

    }

    "consumer method call is lazy if F[_] is lazy" in {
      val consumerTouched: AtomicBoolean = new AtomicBoolean(false)

      val consumer = new ExplodingConsumer {
        override def commitSync(): Unit = consumerTouched.set(true)
      }

      val rc = RebalanceCallback.api[IO].commit

      val io: IO[Unit] = rc.toF(consumer)
      // IO is lazy, so consumer should not have been called at this point
      consumerTouched.get() mustBe false
      io.toTry mustBe Try(())
      // but after running it we should observe the usage of consumer
      consumerTouched.get() mustBe true
    }

  }

  def ioTests[A](
    rc: RebalanceCallbackApi[IO] => RebalanceCallback[IO, A],
    expected: A,
    consumer: RebalanceConsumerJ,
    reset: () => ()              = () => (),
    verifyOtherResults: () => () = () => ()
  ): Unit = {
    reset()
    rc(RebalanceCallback.api).run(consumer) mustBe Try(expected)
    verifyOtherResults()

    reset()
    rc(RebalanceCallback.api).mapK(ToTry.functionK).run(consumer) mustBe Try(expected)
    verifyOtherResults()

    reset()
    rc(RebalanceCallback.api).toF(consumer).unsafeRunSync() mustBe expected
    verifyOtherResults()
  }

  def ioErrorTests(
    rc: RebalanceCallback[IO, Unit],
    expected: Throwable,
    consumer: RebalanceConsumerJ,
    reset: () => ()              = () => (),
    verifyOtherResults: () => () = () => ()
  ): Unit = {
    reset()
    rc.run(consumer) mustBe Failure(expected)
    verifyOtherResults()

    reset()
    rc.mapK(ToTry.functionK).run(consumer) mustBe Failure(expected)
    verifyOtherResults()

    reset()
    val io = rc.toF(consumer)
    Try(io.unsafeRunSync()) mustBe Failure(expected)
    verifyOtherResults()
  }

}

object RebalanceCallbackSpec {

  def tryRun[A](rc: RebalanceCallback[Try, A], consumer: RebalanceConsumerJ): Try[Any] = {
    rc.run(consumer)
  }

  case object TestError extends NoStackTrace

  case object TestError2 extends NoStackTrace

  def triggerStackOverflowError(n: Int): Int = {
    if (n <= 0) n
    else n + triggerStackOverflowError(n - 1)
  }
}
