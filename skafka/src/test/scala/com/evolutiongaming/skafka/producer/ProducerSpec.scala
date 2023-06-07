package com.evolutiongaming.skafka.producer

import java.util
import java.util.concurrent.CompletableFuture
import cats.arrow.FunctionK
import cats.data.{NonEmptyMap => Nem}
import cats.effect.IO
import cats.implicits._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.MeasureDuration
import com.evolutiongaming.skafka.IOMatchers._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Bytes, OffsetAndMetadata, Partition, PartitionInfo, TopicPartition}
import com.evolutiongaming.skafka.IOSuite._
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata => ConsumerGroupMetadataJ, OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, ProducerRecord => ProducerRecordJ}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition => TopicPartitionJ}

import scala.jdk.CollectionConverters._
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

    "proxy sendOffsetsToTransaction" in new Scope {
      val consumerGroupId = "consumerGroupId"
      val offsets         = Nem.of((topicPartition, OffsetAndMetadata.empty))
      verify(producer.sendOffsetsToTransaction(offsets, consumerGroupId)) { _ =>
        sendOffsetsToTransaction shouldEqual consumerGroupId
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
      verify(producer.partitions(topic)) { result: List[PartitionInfo] =>
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

    implicit val empty = Producer.empty[IO]

    "initTransactions" in {
      verify(Producer[IO].initTransactions) { _ => }
    }

    "beginTransaction" in new Scope {
      verify(empty.beginTransaction) { _ => }
    }

    "sendOffsetsToTransaction" in new Scope {
      val offsets = Nem.of((topicPartition, OffsetAndMetadata.empty))
      verify(empty.sendOffsetsToTransaction(offsets, "consumerGroupId")) { _ => }
    }

    "commitTransaction" in new Scope {
      verify(empty.commitTransaction) { _ => }
    }

    "abortTransaction" in new Scope {
      verify(empty.abortTransaction) { _ => }
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
    var flushCalled               = false
    var commitTransaction         = false
    var beginTransaction          = false
    var initTransactions          = false
    var abortTransaction          = false
    var partitionsFor             = ""
    var sendOffsetsToTransaction  = ""
    var sendOffsetsToTransaction1 = none[ConsumerGroupMetadataJ]
    val completableFuture         = CompletableFuture.completedFuture(metadata.asJava)

    val jProducer: ProducerJ[Bytes, Bytes] = new ProducerJ[Bytes, Bytes] {

      def initTransactions() = Scope.this.initTransactions = true

      def beginTransaction() = Scope.this.beginTransaction = true

      def sendOffsetsToTransaction(
        offsets: java.util.Map[TopicPartitionJ, OffsetAndMetadataJ],
        consumerGroupId: String
      ) = {
        Scope.this.sendOffsetsToTransaction = consumerGroupId
      }

      def sendOffsetsToTransaction(
        offsets: util.Map[TopicPartitionJ, OffsetAndMetadataJ],
        groupMetadata: ConsumerGroupMetadataJ
      ) = {
        Scope.this.sendOffsetsToTransaction1 = groupMetadata.some
      }

      def flush() = flushCalled = true

      def commitTransaction() = Scope.this.commitTransaction = true

      def partitionsFor(topic: String) = {
        Scope.this.partitionsFor = topic
        Nil.asJava
      }

      def metrics() = Map.empty[MetricName, Metric].asJava

      def close() = {}

      def close(timeout: java.time.Duration) = {}

      def send(record: ProducerRecordJ[Bytes, Bytes]) = completableFuture

      def send(record: ProducerRecordJ[Bytes, Bytes], callback: Callback) = {
        callback.onCompletion(metadata.asJava, null)
        completableFuture
      }

      def abortTransaction() = Scope.this.abortTransaction = true
    }

    val producer: Producer[IO] = {
      implicit val measureDuration = MeasureDuration.empty[IO]
      Producer
        .fromProducerJ2[IO](jProducer.pure[IO])
        .allocated
        .toTry
        .get
        ._1
        .withMetrics1(ProducerMetrics.empty[IO].mapK(FunctionK.id))
        .mapK(FunctionK.id, FunctionK.id)
    }
  }
}
