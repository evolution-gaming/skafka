package com.evolutiongaming.skafka.producer

import java.util
import java.util.concurrent.{CompletableFuture, Future as FutureJ}
import cats.arrow.FunctionK
import cats.data.NonEmptyMap as Nem
import cats.effect.IO
import cats.implicits.*
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.catshelper.CatsHelper.*
import com.evolutiongaming.skafka.IOMatchers.*
import com.evolutiongaming.skafka.consumer.ConsumerGroupMetadata
import com.evolutiongaming.skafka.producer.ProducerConverters.*
import com.evolutiongaming.skafka.{Bytes, OffsetAndMetadata, Partition, PartitionInfo, TopicPartition}
import com.evolutiongaming.skafka.IOSuite.*
import org.apache.kafka.clients.consumer.{
  ConsumerGroupMetadata as ConsumerGroupMetadataJ,
  OffsetAndMetadata as OffsetAndMetadataJ
}
import org.apache.kafka.clients.producer.{
  Callback,
  Producer as ProducerJ,
  ProducerRecord as ProducerRecordJ,
  RecordMetadata as RecordMetadataJ
}
import org.apache.kafka.common
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.{Metric, MetricName, Uuid, TopicPartition as TopicPartitionJ}

import scala.jdk.CollectionConverters.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProducerSpec extends AnyWordSpec with Matchers {

  val topic          = "topic"
  val topicPartition = TopicPartition(topic = topic, partition = Partition.min)
  val metadata       = RecordMetadata(topicPartition)

  "Producer" should {

    "proxy initTransactions" in new Scope {
      verify(producer.initTransactions) { _ =>
        initTransactions shouldEqual true
      }
    }

    "proxy beginTransaction" in new Scope {
      verify(producer.beginTransaction) { _ =>
        beginTransaction shouldEqual true
      }
    }

    "proxy commitTransaction" in new Scope {
      verify(producer.commitTransaction) { _ =>
        commitTransaction shouldEqual true
      }
    }

    "proxy abortTransaction" in new Scope {
      verify(producer.abortTransaction) { _ =>
        abortTransaction shouldEqual true
      }
    }

    "proxy sendOffsetsToTransaction" in new Scope {
      val offsets       = Nem.of(topicPartition -> OffsetAndMetadata())
      val groupMetadata = ConsumerGroupMetadata("group", 1, "member", None)
      verify(producer.sendOffsetsToTransaction(offsets, groupMetadata)) { _ =>
        sendOffsetsToTransaction1.map(_.groupId) shouldEqual Some("group")
      }
    }

    "proxy send" in new Scope {
      val record = ProducerRecord(topic = topic, value = "val", key = "key")
      producer.send(record).flatten should produce(metadata)
    }

    "proxy sendNoVal" in new Scope {
      val record = ProducerRecord(topic = topic, key = Some("key"))
      producer.sendNoVal(record).flatten should produce(metadata)
    }

    "proxy sendNoKey" in new Scope {
      val record = ProducerRecord(topic = topic, value = Some("val"))
      producer.sendNoKey(record).flatten should produce(metadata)
    }

    "proxy sendEmpty" in new Scope {
      val record = ProducerRecord(topic = topic)
      producer.sendEmpty(record).flatten should produce(metadata)
    }

    "partitions" in new Scope {
      verify(producer.partitions(topic)) { (result: List[PartitionInfo]) =>
        result shouldEqual Nil
        partitionsFor shouldEqual topic
      }
    }

    "proxy flush" in new Scope {
      flushCalled shouldEqual false
      verify(producer.flush) { _ =>
        flushCalled shouldEqual true
      }
    }
  }

  "Producer.empty" should {

    implicit val empty: Producer[IO] = Producer.empty[IO]

    "initTransactions" in {
      verify(Producer[IO].initTransactions) { _ => }
    }

    "beginTransaction" in new Scope {
      verify(empty.beginTransaction) { _ => }
    }

    "commitTransaction" in new Scope {
      verify(empty.commitTransaction) { _ => }
    }

    "abortTransaction" in new Scope {
      verify(empty.abortTransaction) { _ => }
    }

    "sendOffsetsToTransaction" in new Scope {
      val offsets = Nem.of(topicPartition -> OffsetAndMetadata())
      verify(empty.sendOffsetsToTransaction(offsets, ConsumerGroupMetadata("group", 1, "member", None))) { _ => }
    }

    "send" in {
      val record = ProducerRecord(topic = topic, value = "val", key = "key")
      empty.send(record).flatten should produce(metadata)
    }

    "partitions" in new Scope {
      empty.partitions(topic) should produce(List.empty[PartitionInfo])
    }

    "flush" in {
      verify(empty.flush) { _ => }
    }
  }

  private trait Scope {
    var flushCalled                                               = false
    var commitTransaction                                         = false
    var beginTransaction                                          = false
    var initTransactions                                          = false
    var abortTransaction                                          = false
    var partitionsFor                                             = ""
    var sendOffsetsToTransaction                                  = ""
    var sendOffsetsToTransaction1: Option[ConsumerGroupMetadataJ] = none[ConsumerGroupMetadataJ]
    val completableFuture: CompletableFuture[RecordMetadataJ]     = CompletableFuture.completedFuture(metadata.asJava)

    val jProducer: ProducerJ[Bytes, Bytes] = new ProducerJ[Bytes, Bytes] {

      def initTransactions(): Unit = Scope.this.initTransactions = true

      def beginTransaction(): Unit = Scope.this.beginTransaction = true

      def sendOffsetsToTransaction(
        offsets: util.Map[TopicPartitionJ, OffsetAndMetadataJ],
        groupMetadata: ConsumerGroupMetadataJ
      ): Unit = {
        Scope.this.sendOffsetsToTransaction1 = groupMetadata.some
      }

      def flush(): Unit = flushCalled = true

      def commitTransaction(): Unit = Scope.this.commitTransaction = true

      def partitionsFor(topic: String): util.List[common.PartitionInfo] = {
        Scope.this.partitionsFor = topic
        Nil.asJava
      }

      def clientInstanceId(timeout: java.time.Duration): Uuid = Uuid.ONE_UUID

      def metrics(): util.Map[MetricName, Metric] = Map.empty.asJava

      def close(): Unit = {}

      def close(timeout: java.time.Duration): Unit = {}

      def registerMetricForSubscription(metric: KafkaMetric): Unit = {}

      def unregisterMetricFromSubscription(metric: KafkaMetric): Unit = {}

      def send(record: ProducerRecordJ[Bytes, Bytes]): FutureJ[RecordMetadataJ] = completableFuture

      def send(record: ProducerRecordJ[Bytes, Bytes], callback: Callback): FutureJ[RecordMetadataJ] = {
        callback.onCompletion(metadata.asJava, null)
        completableFuture
      }

      def abortTransaction(): Unit = Scope.this.abortTransaction = true
    }

    val producer: Producer[IO] = {
      implicit val measureDuration: MeasureDuration[IO] = MeasureDuration.empty[IO]
      Producer
        .fromProducerJ2[IO](jProducer.pure[IO])
        .allocated
        .toTry
        .get
        ._1
        .withMetrics(ProducerMetrics.empty[IO].mapK(FunctionK.id, FunctionK.id))
        .mapK(FunctionK.id, FunctionK.id)
    }
  }
}
