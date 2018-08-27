package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestKitExtension}
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.producer._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Await

class ProducerConsumerSpec extends FunSuite with BeforeAndAfterAll with Matchers {
  import ProducerConsumerSpec._

  implicit lazy val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  lazy val shutdown = StartKafka()

  override def beforeAll() = {
    super.beforeAll()
    shutdown
  }

  override def afterAll() = {
    shutdown()
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val timeout = TestKitExtension(system).DefaultTimeout.duration

  val headers = List(Header(key = "key", value = "value".getBytes(UTF_8)))

  def producers(acks: Acks) = {
    val ecBlocking = system.dispatcher
    val config = ProducerConfig.Default.copy(acks = acks)
    List(
      CreateProducer(config, ecBlocking),
      CreateProducer(config, ecBlocking, system))
  }

  for {
    acks <- List(Acks.One, Acks.None)
    (producer, idx) <- producers(acks).zipWithIndex
  } yield {

    val topic = s"$idx-$acks"
    val name = s"[topic:$topic,acks:$acks]"

    def produce(record: ProducerRecord[String, String]) = {
      val future = producer.send(record)
      Await.result(future, timeout)
    }

    lazy val consumer = {
      val config = ConsumerConfig.Default.copy(
        groupId = Some(s"group-$topic"),
        autoOffsetReset = AutoOffsetReset.Earliest)

      val deserializer = FromBytes.StringFromBytes.asJava
      val consumer = new KafkaConsumer(config.properties, deserializer, deserializer)
      consumer.subscribe(List(topic).asJavaCollection)
      consumer
    }

    test(s"$name produce and consume record") {
      val key = "key1"
      val value = "value1"
      val timestamp = Instant.now()
      val record = ProducerRecord(
        topic = topic,
        value = Some(value),
        key = Some(key),
        timestamp = Some(timestamp),
        headers = headers)
      val metadata = produce(record)
      val offset = if (acks == Acks.None) None else Some(0l)
      metadata.offset shouldEqual offset

      val records = consume().map(Record(_))

      val expected = Record(
        record = ConsumerRecord(
          topicPartition = metadata.topicPartition,
          offset = 0l,
          timestampAndType = Some(TimestampAndType(timestamp, TimestampType.Create)),
          key = Some(WithSize(key, 4)),
          value = Some(WithSize(value, 6)),
          headers = Nil),
        headers = List(Record.Header(key = "key", value = "value")))

      records shouldEqual List(expected)
    }

    test(s"$name produce and delete record") {
      val key = "key2"
      val record = ProducerRecord(topic, Some("value2"), Some(key))
      val metadata = produce(record)
      val offset = if (acks == Acks.None) None else Some(1l)
      metadata.offset shouldEqual offset

      val keyAndValues = consume().map { record => (record.key.map(_.value), record.value.map(_.value)) }
      keyAndValues shouldEqual List((Some(key), record.value))

      val timestamp = Instant.now()
      val delete = ProducerRecord[Bytes, Bytes](
        topic = topic,
        key = Some(key.getBytes("UTF-8")),
        timestamp = Some(timestamp),
        headers = headers)

      val javaProducer = CreateJProducer(ProducerConfig.Default.copy(acks = acks))
      val deleteMetadata = javaProducer.send(delete.asJava).get().asScala

      val records = consume().map(Record(_))

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
      val timestamp = Instant.now()
      val empty = ProducerRecord[Bytes, Bytes](
        topic = topic,
        timestamp = Some(timestamp),
        headers = headers)

      val javaProducer = CreateJProducer(ProducerConfig.Default.copy(acks = acks))
      val metadata = javaProducer.send(empty.asJava).get().asScala

      val records = consume().map(Record(_))

      val expected = Record(
        record = ConsumerRecord[String, String](
          topicPartition = metadata.topicPartition,
          offset = 3l,
          timestampAndType = Some(TimestampAndType(timestamp, TimestampType.Create)),
          headers = Nil),
        headers = List(Record.Header(key = "key", value = "value")))

      records shouldEqual List(expected)
    }

    def consume(): List[ConsumerRecord[String, String]] = {
      @tailrec
      def consume(retries: Int): List[ConsumerRecord[String, String]] = {
        if (retries <= 0) Nil
        else {
          val recordsJava = consumer.poll(100)
          val records = recordsJava.asScala.values.values.flatten
          if (records.isEmpty) consume(retries - 1)
          else records.toList
        }
      }

      consume(100)
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
}