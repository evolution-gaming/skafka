package com.evolutiongaming.skafka.producer

import java.util.concurrent.{CompletableFuture, TimeUnit}

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.producer.Producer.RecordMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, Producer => JProducer}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Success

class ProducerFactorySpec extends WordSpec with Matchers {

  "ProducerFactory" should {

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

    "proxy closeAsync" in new Scope {
      closeTimeout shouldEqual None
      val timeout = 3.seconds
      producer.closeAsync(timeout)
      closeTimeout shouldEqual Some(timeout)
    }

    "proxy send" in new Scope {
      val record = Producer.Record(topic = topic, value = "0")
      val result = producer.send(record)
      result.value shouldEqual Some(Success(metadata))
    }
  }

  private trait Scope {
    var flushCalled = false
    var closeCalled = false
    var closeTimeout = Option.empty[FiniteDuration]
    val topic = "topic"
    val metadata = RecordMetadata(topic = topic, partition = 0)
    val completableFuture = CompletableFuture.completedFuture(metadata.asJava)

    val jProducer = new JProducer[Int, String] {
      def sendOffsetsToTransaction(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {}
      def initTransactions() = {}
      def beginTransaction() = {}
      def flush() = { flushCalled = true }
      def commitTransaction() = {}
      def partitionsFor(topic: String) = Nil.asJava
      def metrics() = Map.empty[MetricName, Metric].asJava
      def close() = closeCalled = true
      def close(timeout: Long, unit: TimeUnit) = closeTimeout = Some(FiniteDuration(timeout, unit))
      def send(record: ProducerRecord[Int, String]) = completableFuture
      def send(record: ProducerRecord[Int, String], callback: Callback) = completableFuture
      def abortTransaction() = {}
    }

    val producer = ProducerFactory(jProducer, CurrentThreadExecutionContext)
  }
}
