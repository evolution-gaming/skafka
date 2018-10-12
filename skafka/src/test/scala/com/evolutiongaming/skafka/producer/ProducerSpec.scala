package com.evolutiongaming.skafka.producer

import java.util.concurrent.{CompletableFuture, TimeUnit}

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.concurrent.sequentially.SequentiallyHandler
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Bytes, TopicPartition}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, ProducerRecord => ProducerRecordJ}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition => TopicPartitionJ}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success

class ProducerSpec extends WordSpec with Matchers {

  val topic = "topic"
  val topicPartition = TopicPartition(topic = topic, partition = 0)
  val metadata = RecordMetadata(topicPartition)

  "CreateProducer" should {

    "proxy initTransactions" in new Scope {
      producer.initTransactions()
      initTransactions shouldEqual true
    }

    "proxy beginTransaction" in new Scope {
      producer.beginTransaction()
      beginTransaction shouldEqual true
    }

    "proxy sendOffsetsToTransaction" in new Scope {
      val consumerGroupId = "consumerGroupId"
      producer.sendOffsetsToTransaction(Map.empty, consumerGroupId).value shouldEqual Some(Success(()))
      sendOffsetsToTransaction shouldEqual consumerGroupId
    }

    "proxy commitTransaction" in new Scope {
      producer.commitTransaction()
      commitTransaction shouldEqual true
    }

    "proxy abortTransaction" in new Scope {
      producer.abortTransaction()
      abortTransaction shouldEqual true
    }

    "proxy send" in new Scope {
      val record = ProducerRecord(topic = topic, value = "val", key = "key")
      val result = producer.send(record)
      result.value shouldEqual Some(Success(metadata))
    }

    "proxy sendNoVal" in new Scope {
      val record = ProducerRecord(topic = topic, key = Some("key"))
      val result = producer.sendNoVal(record)
      result.value shouldEqual Some(Success(metadata))
    }

    "proxy sendNoKey" in new Scope {
      val record = ProducerRecord(topic = topic, value = Some("val"))
      val result = producer.sendNoKey(record)
      result.value shouldEqual Some(Success(metadata))
    }

    "proxy sendEmpty" in new Scope {
      val record = ProducerRecord(topic = topic)
      val result = producer.sendEmpty(record)
      result.value shouldEqual Some(Success(metadata))
    }

    "partitions" in new Scope {
      producer.partitions(topic).value shouldEqual Some(Success(Nil))
      partitionsFor shouldEqual topic
    }

    "proxy flush" in new Scope {
      flushCalled shouldEqual false
      val result = producer.flush()
      flushCalled shouldEqual true
      result.value shouldEqual Some(Success(()))
    }

    "proxy close" in new Scope {
      closeCalled shouldEqual false
      producer.close()
      closeCalled shouldEqual true
    }

    "proxy close with timeout" in new Scope {
      closeTimeout shouldEqual None
      val timeout = 3.seconds
      producer.close(timeout)
      closeTimeout shouldEqual Some(timeout)
    }
  }


  "CreateProducer.Empty" should {

    "initTransactions" in {
      Producer.Empty.initTransactions() shouldEqual Future.unit
    }

    "beginTransaction" in new Scope {
      Producer.Empty.beginTransaction()
    }

    "sendOffsetsToTransaction" in new Scope {
      Producer.Empty.sendOffsetsToTransaction(Map.empty, "consumerGroupId") shouldEqual Future.unit
    }

    "commitTransaction" in new Scope {
      Producer.Empty.commitTransaction() shouldEqual Future.unit
    }

    "abortTransaction" in new Scope {
      Producer.Empty.abortTransaction() shouldEqual Future.unit
    }

    "send" in {
      val record = ProducerRecord(topic = topic, value = "val", key = "key")
      val result = Producer.Empty.send(record)
      result.value shouldEqual Some(Success(metadata))
    }

    "partitions" in new Scope {
      Producer.Empty.partitions(topic) shouldEqual Future.nil
    }

    "close" in {
      Producer.Empty.close() shouldEqual Future.unit
    }

    "flush" in {
      Producer.Empty.flush() shouldEqual Future.unit
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
    val producer = {
      val producer = Producer(jProducer, SequentiallyHandler.now, ec)
      Producer(producer, Producer.Metrics.Empty)
    }
  }
}
