package com.evolutiongaming.skafka.producer

import java.util.concurrent.CompletableFuture

import cats.arrow.FunctionK
import cats.effect.{Concurrent, IO}
import cats.implicits._
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.skafka.IOMatchers._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Blocking, Bytes, PartitionInfo, TopicPartition}
import com.evolutiongaming.smetrics.MeasureDuration
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, ProducerRecord => ProducerRecordJ}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition => TopicPartitionJ}
import org.scalatest.{Matchers, WordSpec}

import scala.jdk.CollectionConverters._

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
    var flushCalled = false
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
      implicit val executor = CurrentThreadExecutionContext
      implicit val cs = IO.contextShift(executor)
      implicit val concurrentIO: Concurrent[IO]     = IO.ioConcurrentEffect
      implicit val measureDuration = MeasureDuration.empty[IO]
      Producer[IO](jProducer, Blocking(executor))
        .withMetrics(ProducerMetrics.empty)
        .mapK(FunctionK.id, FunctionK.id)
    }
  }
}
