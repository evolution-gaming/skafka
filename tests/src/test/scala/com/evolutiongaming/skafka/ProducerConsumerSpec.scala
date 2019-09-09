package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import cats.arrow.FunctionK
import cats.data.{NonEmptyList => Nel}
import cats.effect.{IO, Resource}
import cats.implicits._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.producer._
import com.evolutiongaming.skafka.IOSuite._
import com.evolutiongaming.smetrics.CollectorRegistry
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._

class ProducerConsumerSpec extends FunSuite with BeforeAndAfterAll with Matchers {
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

  def producers(acks: Acks) = {
    val config = ProducerConfig.Default.copy(acks = acks)
    val producer = for {
      metrics    <- ProducerMetrics.of(CollectorRegistry.empty[IO])
      producerOf  = ProducerOf(executor, metrics("clientId").some).mapK(FunctionK.id, FunctionK.id)
      producer   <- producerOf(config)
    } yield {
      producer.withLogging(Log.empty)
    }
    List(producer.allocated.unsafeRunSync())
  }

  lazy val combinations: Seq[(Acks, List[(Producer[IO], IO[Unit])])] = for {
    acks <- List(Acks.One, Acks.None)
  } yield (acks, producers(acks))

  for {
    (acks, producers)    <- combinations
    ((producer, _), idx) <- producers.zipWithIndex
  } yield {

    val topic = s"$idx-$acks"
    val name = s"[topic:$topic,acks:$acks]"

    def produce(record: ProducerRecord[String, String]) = producer.send(record).flatten.unsafeRunSync()

    lazy val (consumer, consumerRelease) = consumerOf()

    def consumerOf(): (Consumer[IO, String, String], IO[Unit]) = {

      val config = ConsumerConfig.Default.copy(
        groupId = Some(s"group-$topic"),
        autoOffsetReset = AutoOffsetReset.Earliest,
        autoCommit = false,
        common = CommonConfig(clientId = Some(UUID.randomUUID().toString)))

      val consumer = for {
        metrics    <- ConsumerMetrics.of(CollectorRegistry.empty[IO])
        consumerOf  = ConsumerOf[IO](executor, metrics("clientId").some).mapK(FunctionK.id, FunctionK.id)
        consumer   <- consumerOf[String, String](config)
        _          <- Resource.liftF(consumer.subscribe(Nel.of(topic), None))
      } yield consumer
      consumer.allocated.unsafeRunSync()
    }

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
      val offset = if (acks == Acks.None) None else Some(Offset.Min)
      metadata.offset shouldEqual offset

      val records = consumer.consume(timeout).map(Record(_))

      val expected = Record(
        record = ConsumerRecord(
          topicPartition = metadata.topicPartition,
          offset = Offset.Min,
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
      val offset = if (acks == Acks.None) None else Some(1l)
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
          offset = 2l,
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
          offset = 3l,
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

      val (consumer2, consumer2Release) = consumerOf()

      val records = consumer2.consume(timeout).map(Record(_))

      val expected = Record(
        record = ConsumerRecord(
          topicPartition = metadata.topicPartition,
          offset = 4l,
          timestampAndType = Some(TimestampAndType(timestamp, TimestampType.Create)),
          key = Some(WithSize(key, 4)),
          value = Some(WithSize(value, 6)),
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