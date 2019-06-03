package com.evolutiongaming.skafka.producer

import java.util.concurrent.{CompletableFuture, TimeUnit}

import cats.effect.IO
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.skafka.IOMatchers._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Bytes, PartitionInfo, TopicPartition}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, ProducerRecord => ProducerRecordJ}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition => TopicPartitionJ}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class ProducerSpec extends WordSpec with Matchers {

  val topic = "topic"
  val topicPartition = TopicPartition(topic = topic, partition = 0)
  val metadata = RecordMetadata(topicPartition)

  "CreateProducer" should {

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
      verify(producer.sendOffsetsToTransaction(Map.empty, consumerGroupId)) { _ =>
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
      producer.send(record) should produce(metadata)
    }

    "proxy sendNoVal" in new Scope {
      val record = ProducerRecord(topic = topic, key = Some("key"))
      producer.sendNoVal(record) should produce(metadata)
    }

    "proxy sendNoKey" in new Scope {
      val record = ProducerRecord(topic = topic, value = Some("val"))
      producer.sendNoKey(record) should produce(metadata)
    }

    "proxy sendEmpty" in new Scope {
      val record = ProducerRecord(topic = topic)
      producer.sendEmpty(record) should produce(metadata)
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

    "proxy close" in new Scope {
      closeCalled shouldEqual false
      verify(producer.close) { _ =>
        closeCalled shouldEqual true
      }
    }

    "proxy close with timeout" in new Scope {
      closeTimeout shouldEqual None
      val timeout = 3.seconds
      verify(producer.close(timeout)) { _ =>
        closeTimeout shouldEqual Some(timeout)
      }
    }
  }


  "CreateProducer.empty" should {

    implicit val empty = Producer.empty[IO]

    "initTransactions" in {
      verify(Producer[IO].initTransactions) { _ => }
    }

    "beginTransaction" in new Scope {
      verify(empty.beginTransaction) { _ => }
    }

    "sendOffsetsToTransaction" in new Scope {
      verify(empty.sendOffsetsToTransaction(Map.empty, "consumerGroupId")) { _ => }
    }

    "commitTransaction" in new Scope {
      verify(empty.commitTransaction) { _ => }
    }

    "abortTransaction" in new Scope {
      verify(empty.abortTransaction) { _ => }
    }

    "send" in {
      val record = ProducerRecord(topic = topic, value = "val", key = "key")
      empty.send(record) should produce(metadata)
    }

    "partitions" in new Scope {
      empty.partitions(topic) should produce(List.empty[PartitionInfo])
    }

    "close" in {
      verify(empty.close) { _ => }
    }

    "flush" in {
      verify(empty.flush) { _ => }
    }
  }

  private trait Scope {
    var flushCalled = false
    var closeCalled = false
    var closeTimeout = Option.empty[FiniteDuration]
    var commitTransaction = false
    var beginTransaction = false
    var initTransactions = false
    var abortTransaction = false
    var partitionsFor = ""
    var sendOffsetsToTransaction = ""
    val completableFuture = CompletableFuture.completedFuture(metadata.asJava)

    val jProducer = new ProducerJ[Bytes, Bytes] {
      def sendOffsetsToTransaction(offsets: java.util.Map[TopicPartitionJ, OffsetAndMetadataJ], consumerGroupId: String) = {
        Scope.this.sendOffsetsToTransaction = consumerGroupId
      }

      def initTransactions() = Scope.this.initTransactions = true

      def beginTransaction() = Scope.this.beginTransaction = true

      def flush() = flushCalled = true

      def commitTransaction() = Scope.this.commitTransaction = true

      def partitionsFor(topic: String) = {
        Scope.this.partitionsFor = topic
        Nil.asJava
      }

      def metrics() = Map.empty[MetricName, Metric].asJava

      def close() = closeCalled = true

      def close(timeout: Long, unit: TimeUnit) = closeTimeout = Some(FiniteDuration(timeout, unit))

      def send(record: ProducerRecordJ[Bytes, Bytes]) = completableFuture

      def send(record: ProducerRecordJ[Bytes, Bytes], callback: Callback) = {
        callback.onCompletion(metadata.asJava, null)
        completableFuture
      }

      def abortTransaction() = Scope.this.abortTransaction = true
    }
    val ec = CurrentThreadExecutionContext
    val producer: Producer[IO] = {
      implicit val ec = CurrentThreadExecutionContext
      implicit val cs = IO.contextShift(ec)
      implicit val producer = Producer[IO](jProducer, ec)
      Producer[IO]
    }
  }
}
