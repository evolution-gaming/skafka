package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.{Duration => DurationJ}
import java.util.concurrent.atomic.AtomicReference
import java.util.{Collection => CollectionJ, List => ListJ, Map => MapJ, Set => SetJ}

import cats.effect.IO
import com.evolutiongaming.skafka.Converters.{SkafkaOffsetAndMetadataOpsConverters, TopicPartitionOps}
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.DataPoints._
import com.evolutiongaming.skafka.consumer.RebalanceCallback._
import com.evolutiongaming.skafka.consumer.RebalanceCallbackSpec._
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata => ConsumerGroupMetadataJ, OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.common.{PartitionInfo => PartitionInfoJ, TopicPartition => TopicPartitionJ}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Try}

class RebalanceCallbackSpec extends AnyFreeSpec with Matchers {

  "RebalanceCallback" - {

    "consumer unrelated methods do nothing with consumer" - {
      val consumer = new ExplodingConsumer

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
        val output = partitions.s

        val consumer = new ExplodingConsumer {
          override def assignment(): SetJ[TopicPartitionJ] = partitions.j
        }

        tryRun(assignment, consumer) mustBe Try(output)
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
      }

      "groupMetadata" in {
        val output = consumerGroupMetadata

        val consumer = new ExplodingConsumer {
          override def groupMetadata(): ConsumerGroupMetadataJ = output.j
        }

        tryRun(groupMetadata, consumer) mustBe Try(output.s)
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
      }

      "paused" in {
        val output = partitions

        val consumer = new ExplodingConsumer {
          override def paused(): SetJ[TopicPartitionJ] = output.j
        }

        tryRun(paused, consumer) mustBe Try(output.s)
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
      }

      "subscription" in {
        val output = partitions.s.map(_.topic).toSortedSet

        val consumer = new ExplodingConsumer {
          override def subscription(): SetJ[String] = output.asJava
        }

        tryRun(subscription, consumer) mustBe Try(output)
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
              a.set("step-before-rcError1")
            })
            _ <- lift(IO.raiseError[Unit](TestError)) // should fail the execution
            _ <- paused // paused throws TestError2, should not overwrite first error from lift
            _ <- lift(IO.delay {
              b.set("this change should not happen")
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

          RebalanceCallback.run(rcOk, new ExplodingConsumer) mustBe Try(())
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

          tryRun(rc, new ExplodingConsumer) mustBe Try(())
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
