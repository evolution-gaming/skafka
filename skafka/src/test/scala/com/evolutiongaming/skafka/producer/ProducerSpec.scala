package com.evolutiongaming.skafka.producer

import java.util.concurrent.{CompletableFuture, TimeUnit}

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.sequentially.SequentiallyHandler
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Bytes, TopicPartition}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.clients.producer.{Callback, Producer => JProducer, ProducerRecord => ProducerRecordJ}
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
  }


  "CreateProducer.Empty" should {
    
    "send" in {
      val record = ProducerRecord(topic = topic, value = "val", key = "key")
      val result = Producer.Empty.send(record)
      result.value shouldEqual Some(Success(metadata))
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
    val completableFuture = CompletableFuture.completedFuture(metadata.asJava)

    val jProducer = new JProducer[Bytes, Bytes] {
      def sendOffsetsToTransaction(offsets: java.util.Map[TopicPartitionJ, OffsetAndMetadataJ], consumerGroupId: String) = {}
      def initTransactions() = {}
      def beginTransaction() = {}
      def flush() = { flushCalled = true }
      def commitTransaction() = {}
      def partitionsFor(topic: String) = Nil.asJava
      def metrics() = Map.empty[MetricName, Metric].asJava
      def close() = closeCalled = true
      def close(timeout: Long, unit: TimeUnit) = closeTimeout = Some(FiniteDuration(timeout, unit))
      def send(record: ProducerRecordJ[Bytes, Bytes]) = completableFuture
      def send(record: ProducerRecordJ[Bytes, Bytes], callback: Callback) = {
        callback.onCompletion(metadata.asJava, null)
        completableFuture
      }
      def abortTransaction() = {}
    }
    val ec = CurrentThreadExecutionContext
    val producer = Producer(jProducer, SequentiallyHandler.now, ec)
  }
}
