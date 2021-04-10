package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import cats.arrow.FunctionK
import cats.data.{NonEmptySet => Nes}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{IO, Resource}
import cats.implicits._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.skafka.FiberWithBlockingCancel._
import com.evolutiongaming.skafka.IOSuite._
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.producer._
import com.evolutiongaming.smetrics.CollectorRegistry
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._

class ProducerConsumerSpec extends AnyFunSuite with BeforeAndAfterAll with Matchers {
  import ProducerConsumerSpec._

  private lazy val shutdown = StartKafka()

  private val instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  private val timeout = 1.minute

  override def beforeAll() = {
    super.beforeAll()
    shutdown
    ()
  }

  override def afterAll() = {

    val producers = for {
      (_, producers) <- combinations.toList
      producer       <- producers
    } yield producer

    val closeAll = producers.distinct.foldMapM { case (producer, release) =>
      for {
        _ <- producer.flush
        _ <- release
      } yield {}
    }

    closeAll.unsafeRunTimed(timeout)

    shutdown()
    super.afterAll()
  }


  val headers = List(Header(key = "key", value = "value".getBytes(UTF_8)))

  def consumerOf(
    topic: Topic,
    listener: Option[RebalanceListener[IO]]
  ): Resource[IO, Consumer[IO, String, String]] = {

    val config = ConsumerConfig.Default.copy(
      groupId = Some(s"group-$topic"),
      autoOffsetReset = AutoOffsetReset.Earliest,
      autoCommit = false,
      common = CommonConfig(clientId = Some(UUID.randomUUID().toString)))

    for {
      metrics    <- ConsumerMetrics.of(CollectorRegistry.empty[IO])
      consumerOf  = ConsumerOf[IO](executor, metrics("clientId").some).mapK(FunctionK.id, FunctionK.id)
      consumer   <- consumerOf[String, String](config)
      _          <- consumer.subscribe(Nes.of(topic), listener).toResource
    } yield consumer
  }

  def producerOf(acks: Acks): Resource[IO, Producer[IO]] = {
    val config = ProducerConfig.Default.copy(acks = acks)
    for {
      metrics    <- ProducerMetrics.of(CollectorRegistry.empty[IO])
      producerOf  = ProducerOf(executor, metrics("clientId").some).mapK(FunctionK.id, FunctionK.id)
      producer   <- producerOf(config)
    } yield {
      producer.withLogging(Log.empty)
    }
  }

  test("rebalance") {
    val topic = s"${instant.toEpochMilli}-rebalance"

    def listenerOf(assigned: Deferred[IO, Unit]): RebalanceListener[IO] = {
      new RebalanceListener[IO] {

        def onPartitionsAssigned(partitions: Nes[TopicPartition]) = assigned.complete(())

        def onPartitionsRevoked(partitions: Nes[TopicPartition]) = ().pure[IO]

        def onPartitionsLost(partitions: Nes[TopicPartition]) = ().pure[IO]
      }
    }

    val result = for {
      assigned <- Deferred[IO, Unit]
      listener  = listenerOf(assigned = assigned)
      consumer  = consumerOf(topic, listener.some)
      _        <- consumer.use { consumer =>
        val poll = consumer
          .poll(10.millis)
          .foreverM[Unit]
        Resource
          .make(poll.start) { _.cancel }
          .use { _ => assigned.get.timeout(1.minute) }
      }
    } yield consumer
    result.unsafeRunSync()
  }

  test("rebalance listener correctness - consumer.position") {
    // there was a bug where skafka's consumer methods would be working incorrectly if used inside rebalance listener
    // two of them are representing world's most common use cases of consumer inside rebalance listener
    // - position
    //   the bug was causing it to return wrong offset, here's an example for a topic with single partition with 6 messages
    //   current committed offset = 3,
    //   consumer.poll is able to fetch 3 more records
    //   consumer.position returns 6 (incorrect) instead of 3 (expected/correct), because it's executed after poll of KafkaConsumer.java
    //
    // - commit
    //   using consumer to commit offsets for revoked partitions results in logical error
    //   as partitions are no longer assigned to this instance of consumer
    //
    //
    // The bug is caused by implementation details of SerialListeners and multi-thread access protection in skafka's consumer
    // specifically because of following points
    // *  6. RebalanceListenerNative exits, while RebalanceListener still running
    // *  7. ConsumerNative.poll exits
    // from https://github.com/evolution-gaming/skafka/blob/b76b257d5f6b2c75564b2bceae5d147d11db424d/skafka/src/main/scala/com/evolutiongaming/skafka/consumer/SerialListeners.scala
    // kafka java consumer rebalance listener API is of a blocking nature, and it's expected from user code to complete the work before exiting listener's methods
    // hence we should not be running skafka's rebalance listener in Async/Background fashion by default,
    // still there's a possibility to fork computation in user land if needed

    // test case:
    // trigger rebalance multiple times by
    //  - creating new consumer instance
    //  - join to consumer group
    //  - shut down consumer on the very first partition assigned
    // in `onPartitionsAssigned` - aggregate consumer.position responses in Set[Offset]
    // if the Set has only one element, then it works correctly

    val topic = s"${instant.toEpochMilli}-rebalance-listener-correctness-position"
    val requiredNumberOfRebalances = 100

    def listenerOf(
      positions: Ref[IO, Set[Offset]],
      rebalanceCounter: Ref[IO, Int],
      assigned: Deferred[IO, Unit]
    ): RebalanceListener1[IO] = {
      new RebalanceListener1[IO] {
        import RebalanceCallback._

        def onPartitionsAssigned(partitions: Nes[TopicPartition]) = {
          for {
            _ <- lift(IO.shift)
            _ <- lift(
              IO.shift *>
                IO.delay(()) *>
                IO.shift *>
                IO.delay(())
            )
            // with IO.shift attempts we make sure that
            // kafka java consumer.position method would be called from the same thread as consumer.poll
            // otherwise kafka java consumer throws
            // ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access")
            // which in result would terminate current flatMap chain
            // and fail the test as positions (Set[Offset]) would be empty
            position <- position(partitions.head) // TODO: allow consumer error handling in user land
            _ <- lift(positions.update(_.+(position)))
            _ <- lift(rebalanceCounter.update(_ + 1))
            _ <- lift(assigned.complete(()))
          } yield ()
        }

        def onPartitionsRevoked(partitions: Nes[TopicPartition]) = noOp

        def onPartitionsLost(partitions: Nes[TopicPartition]) = noOp
      }
    }

    val result = for {
      testCompleted <- Deferred[IO, Unit]
      positions <- Ref.of[IO, Set[Offset]](Set.empty)
      rebalanceCounter <- Ref.of[IO, Int](0)
      completeTestIfNeeded = for {
        rebalanceCounter <- rebalanceCounter.get
        positions <- positions.get
        completed <- if (rebalanceCounter >= requiredNumberOfRebalances || positions.size > 1) testCompleted.complete(()).handleError(_ => ()) else IO.unit
      } yield completed

      consumer = consumerOf(topic, none)
      producer = producerOf(Acks.One)

      _ <- producer.use { producer =>
        for {
          _ <- producer.send(ProducerRecord(topic, "value1", "key")).flatten
          _ <- producer.send(ProducerRecord(topic, "value2", "key")).flatten
        } yield ()
      }

      testStep = Deferred[IO, Unit].flatMap { assigned =>
        val x = for {
          _ <- Resource.release(completeTestIfNeeded)
          consumer <- consumer
          listener = listenerOf(positions, rebalanceCounter, assigned)
          _ <- consumer.subscribe(Nes.of(topic), listener).toResource
          poll = {
              consumer.poll(10.millis).onError({ case e => IO.delay(println(s"${System.nanoTime()} poll failed $e")) })
          }
          _ <- (IO.cancelBoundary *> poll).foreverM.backgroundAwaitExit.withTimeoutRelease(5.seconds)
        } yield ()
        x.use(_ => assigned.get.timeout(10.seconds))
      }

      _ <- Resource
        .make(testStep.foreverM.start) {_.cancel}
        .use { _ =>
          testCompleted.get.timeout(33.seconds)
            .onError({ case _ => rebalanceCounter.get.map(c =>
              println(s"rebalance listener correctness (position): onPartitionsAssigned $c out of $requiredNumberOfRebalances times"))
            })
        }
      positions <- positions.get
    } yield positions

    // we don't have any offsets committed, and only 2 messages are present in input topic
    // also in this test we are not doing consumer.commit
    // therefore consumer.position should always return Offset.min in listener.onPartitionsAssigned
    result.unsafeRunSync() shouldEqual Set(Offset.min)
  }

  test("rebalance listener correctness - consumer.commit") {
    // test case:
    // trigger rebalance multiple times N by
    //  - creating new consumer instance
    //  - join to consumer group
    //  - shut down consumer on the very first partition assigned
    // in `onPartitionsRevoked` - aggregate consumer.committed responses in List[Offset]
    // in `onPartitionsRevoked` - commit single offset within range [1..N)
    // if the List == [0..N), then it works correctly,
    // as every commit was successfully executed

    val topic = s"${instant.toEpochMilli}-rebalance-listener-correctness-commit"
    val requiredNumberOfRebalances = 100

    def listenerOf(
                    offsets: Ref[IO, List[Offset]],
                    rebalanceCounter: Ref[IO, Int],
                    assigned: Deferred[IO, Unit]
                  ): RebalanceListener1[IO] = {
      new RebalanceListener1[IO] {

        import RebalanceCallback._

        def onPartitionsAssigned(partitions: Nes[TopicPartition]) = {
          for {
            _ <- lift(rebalanceCounter.update(_ + 1))
            _ <- lift(assigned.complete(()))
          } yield ()
        }

        def onPartitionsRevoked(partitions: Nes[TopicPartition]) =
          for {
            committed <- committed(partitions)
            offset = committed.headOption.map(_._2.offset).getOrElse(Offset.min)
            _ <- lift(offsets.update(_ :+ offset))
            _ <- commit(partitions.map(_ -> OffsetAndMetadata(Offset.unsafe(offset.value + 1))).toNonEmptyList.toNem)
          } yield ()

        def onPartitionsLost(partitions: Nes[TopicPartition]) = noOp
      }
    }

    val result = for {
      testCompleted <- Deferred[IO, Unit]
      offsets <- Ref.of[IO, List[Offset]](List.empty)
      rebalanceCounter <- Ref.of[IO, Int](0)
      completeTestIfNeeded = for {
        rebalanceCounter <- rebalanceCounter.get
        completed <- if (rebalanceCounter >= requiredNumberOfRebalances) testCompleted.complete(()) else IO.unit
      } yield completed

      consumer = consumerOf(topic, none)
      producer = producerOf(Acks.One)

      _ <- producer.use { producer =>
        // send 1 record just to create the topic
        producer.send(ProducerRecord(topic, s"value", "key")).flatten
      }

      testStep = Deferred[IO, Unit].flatMap { assigned =>
        consumer.use { consumer =>
          val listener = listenerOf(offsets, rebalanceCounter, assigned)
          val poll = consumer.poll(10.millis)
          val subscribe = consumer.subscribe(Nes.of(topic), listener)
          subscribe *>
            Resource
              .make(poll.foreverM.start)(_.cancel)
              .use(_ => assigned.get.timeout(10.seconds)) *>
            completeTestIfNeeded
        }
      }

      _ <- Resource
        .make(testStep.foreverM.start) {
          _.cancel
        }
        .use { _ =>
          testCompleted.get.timeout(33.seconds)
            .onError({ case _ => rebalanceCounter.get.map(c =>
              println(s"rebalance listener correctness (commit): onPartitionsAssigned $c out of $requiredNumberOfRebalances times"))
            })
        }
      offsets <- offsets.get
    } yield offsets

    result.unsafeRunSync() shouldEqual (0 until requiredNumberOfRebalances).map(i => Offset.unsafe(i)).toList
  }

  test("rebalance listener correctness - consumer.seek") {
    // test case:
    // having a topic with 2 messages: key:value1 and key:value2
    // trigger rebalance multiple times by
    //  - creating new consumer instance
    //  - join to consumer group
    //  - shut down consumer after getting some records from poll
    // in `onPartitionsAssigned` - do consumer.seek(1)
    // consumer.poll should always return List(key:value2) as we skip first message using consumer.seek

    val topic = s"${instant.toEpochMilli}-rebalance-listener-correctness-seek"

    val listener: RebalanceListener1[IO] = {
      new RebalanceListener1[IO] {
        import RebalanceCallback._

        def onPartitionsAssigned(partitions: Nes[TopicPartition]) =
          partitions
            .toList.map(seek(_, Offset.unsafe(1)))
            .fold(noOp)((agg, e) => agg.flatMap(_ => e))

        def onPartitionsRevoked(partitions: Nes[TopicPartition]) = noOp

        def onPartitionsLost(partitions: Nes[TopicPartition]) = noOp
      }
    }

    val consumer = consumerOf(topic, none)
    val producer = producerOf(Acks.One)

    producer.use { producer =>
      for {
        _ <- producer.send(ProducerRecord(topic, "value1", "key")).flatten
        _ <- producer.send(ProducerRecord(topic, "value2", "key")).flatten
      } yield ()
    }.unsafeRunSync()

    val consumeRecords: IO[List[(String, String)]] = consumer.use { consumer =>
      for {
        _ <- consumer.subscribe(Nes.of(topic), listener)
        poll = consumer.poll(10.millis)
        records <- poll.iterateUntil(_.values.nonEmpty)
      } yield records.values.values.flatMap(_.toList.map(r => (r.key.get.value, r.value.get.value))).toList
    }

    1 to 100 foreach { _ =>
      consumeRecords.unsafeRunTimed(5.seconds).get shouldEqual List(("key", "value2"))
    }
  }

  lazy val combinations: Seq[(Acks, List[(Producer[IO], IO[Unit])])] = for {
    acks <- List(Acks.One, Acks.None)
  } yield {
    val producer = producerOf(acks)
    (acks, List(producer.allocated.unsafeRunSync()))
  }

  for {
    (acks, producers)    <- combinations
    ((producer, _), idx) <- producers.zipWithIndex
  } yield {

    val topic = s"${instant.toEpochMilli}-$idx-$acks"
    val name = s"[topic:$topic,acks:$acks]"

    def produce(record: ProducerRecord[String, String]) = producer.send(record).flatten.unsafeRunSync()

    lazy val (consumer, consumerRelease) = consumerOf(topic, none)
      .allocated
      .timeout(1.minute)
      .unsafeRunSync()

    test(s"$name produce and consume record") {
      val key = "key1"
      val value = "value1"
      val timestamp = instant
      val record = ProducerRecord(
        topic = topic,
        value = Some(value),
        key = Some(key),
        timestamp = Some(timestamp),
        headers = headers)
      val metadata = produce(record)
      val offset = if (acks == Acks.None) none[Offset] else Offset.min.some
      metadata.offset shouldEqual offset

      val records = consumer.consume(timeout).map(Record(_))

      val expected = Record(
        record = ConsumerRecord(
          topicPartition = metadata.topicPartition,
          offset = Offset.min,
          timestampAndType = Some(TimestampAndType(timestamp, TimestampType.Create)),
          key = Some(WithSize(key, 4)),
          value = Some(WithSize(value, 6)),
          headers = Nil),
        headers = List(Record.Header(key = "key", value = "value")))

      records shouldEqual List(expected)
    }

    test(s"$name produce and delete record") {
      val key = "key2"
      val record = ProducerRecord(topic, value = "value2", key = key)
      val metadata = produce(record)
      val offset = if (acks == Acks.None) none[Offset] else Offset.unsafe(1L).some
      metadata.offset shouldEqual offset

      val keyAndValues = consumer.consume(timeout).map { record => (record.key.map(_.value), record.value.map(_.value)) }
      keyAndValues shouldEqual List((Some(key), record.value))

      val timestamp = instant
      val delete = ProducerRecord[String, String](
        topic = topic,
        key = Some(key),
        timestamp = Some(timestamp),
        headers = headers)

      val deleteMetadata = produce(delete)

      val records = consumer.consume(timeout).map(Record(_))

      val expected = Record(
        record = ConsumerRecord[String, String](
          topicPartition = deleteMetadata.topicPartition,
          offset = Offset.unsafe(2),
          timestampAndType = Some(TimestampAndType(timestamp, TimestampType.Create)),
          key = Some(WithSize(key, 4)),
          headers = Nil),
        headers = List(Record.Header(key = "key", value = "value")))

      records shouldEqual List(expected)
    }

    test(s"$name produce and consume empty record") {
      val timestamp = instant
      val empty = ProducerRecord[String, String](
        topic = topic,
        timestamp = Some(timestamp),
        headers = headers)

      val metadata = produce(empty)

      val records = consumer.consume(timeout).map(Record(_))

      val expected = Record(
        record = ConsumerRecord[String, String](
          topicPartition = metadata.topicPartition,
          offset = Offset.unsafe(3),
          timestampAndType = Some(TimestampAndType(timestamp, TimestampType.Create)),
          headers = Nil),
        headers = List(Record.Header(key = "key", value = "value")))

      records shouldEqual List(expected)
    }

    test(s"$name commit and subscribe from last committed position") {
      val key = "key3"
      val value = "value3"
      val timestamp = instant

      Await.result(consumer.commit(), timeout)
      consumerRelease.unsafeRunSync()

      val record = ProducerRecord(
        topic = topic,
        value = Some(value),
        key = Some(key),
        timestamp = Some(timestamp),
        headers = headers)

      val metadata = produce(record)

      val (consumer2, consumer2Release) = consumerOf(topic, none).allocated.unsafeRunSync()

      val records = consumer2.consume(timeout).map(Record(_))

      val expected = Record(
        record = ConsumerRecord(
          topicPartition = metadata.topicPartition,
          offset = Offset.unsafe(4L),
          timestampAndType = Some(TimestampAndType(timestamp, TimestampType.Create)),
          key = WithSize(key, 4).some,
          value = WithSize(value, 6).some,
          headers = Nil),
        headers = List(Record.Header(key = "key", value = "value")))

      records shouldEqual List(expected)

      consumer2Release.unsafeRunSync()
    }
  }
}

object ProducerConsumerSpec {

  final case class Record(record: ConsumerRecord[String, String], headers: List[Record.Header])

  object Record {

    def apply(record: ConsumerRecord[String, String]): Record = {

      val headers = record.headers.map { header =>
        Header(key = header.key, value = new String(header.value, UTF_8))
      }
      Record(
        record = record.copy(headers = Nil),
        headers = headers)
    }

    final case class Header(key: String, value: String)
  }

  implicit class ConsumersOps(val self: Consumer[IO, String, String]) extends AnyVal {

    def consume(timeout: FiniteDuration): List[ConsumerRecord[String, String]] = {
      @tailrec
      def consume(attempts: Int): List[ConsumerRecord[String, String]] = {
        if (attempts <= 0) Nil
        else {
          val future = self.poll(100.millis).unsafeToFuture()
          val records = Await.result(future, timeout).values.values.flatMap(_.toList)
          if (records.isEmpty) consume(attempts - 1)
          else records.toList
        }
      }

      consume(100)
    }

    def commit() = self.commit.unsafeToFuture()
  }

}