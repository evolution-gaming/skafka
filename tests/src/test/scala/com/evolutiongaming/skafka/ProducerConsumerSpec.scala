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
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.IOSuite._
import com.evolutiongaming.kafka.StartKafka
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
      _          <- Resource.liftF(consumer.subscribe(Nes.of(topic), listener))
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

  test("rebalance listener correctness") {
    // there was a bug where skafka's consumer methods would be working incorrectly if used inside rebalance listener
    // two of them are representing world's most common use cases of consumer inside rebalance listener
    // - position
    //   the bug was causing it to return wrong offset, here's an example for a topic with single partition with 6 messages
    //   current committed offset = 3,
    //   consumer.poll is able to fetch 3 more records
    //   consumer.position returns 6 (incorrect) instead of 3 (expected/correct), because it's executed after poll of KafkaConsumer.java
    //
    // - commit
    //   using consumer to commit offsets for revoked partitions results in error
    //   that partitions are no longer assigned to this instance of consumer
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
    val topic = s"${instant.toEpochMilli}-rebalance-listener-correctness"
    val partitionsAssignedOkThreshold = 100

    def listenerOf(
      consumer: Consumer[IO, String, String],
      positions: Ref[IO, Set[Offset]],
      assignedCounter: Ref[IO, Int],
      assigned: Deferred[IO, Unit]
    ): RebalanceListener[IO] = {
      new RebalanceListener[IO] {

        def onPartitionsAssigned(partitions: Nes[TopicPartition]) = {
          for {
            position <- consumer.position(partitions.head)
            _ <- positions.update(_.+(position))
            _ <- assignedCounter.update(_ + 1)
            _ <- assigned.complete(())
          } yield ()
        }

        def onPartitionsRevoked(partitions: Nes[TopicPartition]) = IO.unit

        def onPartitionsLost(partitions: Nes[TopicPartition]) = IO.unit
      }
    }

    val result = for {
      testCompleted <- Deferred[IO, Unit]
      positions <- Ref.of[IO, Set[Offset]](Set.empty)
      assignedCounter <- Ref.of[IO, Int](0)
      completeTestIfNeeded = for {
        assignedCounter <- assignedCounter.get
        positions <- positions.get
        completed <- if (assignedCounter >= partitionsAssignedOkThreshold || positions.size > 1) testCompleted.complete(()) else IO.unit
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
        consumer.use { consumer =>
          val listener = listenerOf(consumer, positions, assignedCounter, assigned)
          consumer.subscribe(Nes.of(topic), listener.some) *>
            Resource
              .make(consumer.poll(10.millis).foreverM.start)(_.cancel)
              .use(_ => assigned.get.timeout(10.seconds)) *>
            completeTestIfNeeded
        }
      }

      _ <- Resource
        .make(testStep.foreverM.start) {_.cancel}
        .use { _ =>
          testCompleted.get.timeout(33.seconds)
            .onError(_ => assignedCounter.get.map(c =>
              println(s"rebalance listener correctness: onPartitionsAssigned $c out of $partitionsAssignedOkThreshold times")))
        }
      positions <- positions.get
    } yield positions

    // we don't have any offsets committed, and only 2 messages are present in input topic
    // also in this test we are not doing consumer.commit
    // therefore consumer.position should always return Offset.min in listener.onPartitionsAssigned
    result.unsafeRunSync() shouldEqual Set(Offset.min)
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