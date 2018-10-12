package com.evolutiongaming.skafka.producer

import java.time.Instant

import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.TopicPartition
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, ProducerRecord => ProducerRecordJ, RecordMetadata => RecordMetadataJ}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.ProduceResponse

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionException, Future, Promise}
import scala.util.control.NonFatal

object ProducerConverters {

  implicit class ProducerRecordOps[K, V](val self: ProducerRecord[K, V]) extends AnyVal {

    def asJava: ProducerRecordJ[K, V] = {
      new ProducerRecordJ[K, V](
        self.topic,
        self.partition.fold[java.lang.Integer](null) { java.lang.Integer.valueOf },
        self.timestamp.fold[java.lang.Long](null) { timestamp => timestamp.toEpochMilli },
        self.key.getOrElse(null.asInstanceOf[K]),
        self.value.getOrElse(null.asInstanceOf[V]),
        self.headers.map { _.asJava }.asJava)
    }
  }


  implicit class JProducerRecordOps[K, V](val self: ProducerRecordJ[K, V]) extends AnyVal {

    def asScala: ProducerRecord[K, V] = {
      ProducerRecord(
        topic = self.topic,
        value = Option(self.value),
        key = Option(self.key),
        partition = Option(self.partition) map { _.intValue() },
        timestamp = Option(self.timestamp) map { Instant.ofEpochMilli(_) },
        headers = self.headers.asScala.map { _.asScala }.toList
      )
    }
  }


  implicit class ProducerJOps[K, V](val self: ProducerJ[K, V]) extends AnyVal {

    def sendAsScala(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
      val jRecord = record.asJava
      val promise = Promise[RecordMetadata]
      val callback = new Callback {
        def onCompletion(metadata: RecordMetadataJ, failure: Exception) = {
          if (failure != null) {
            promise.failure(failure)
          } else if (metadata != null) {
            promise.success(metadata.asScala)
          } else {
            val failure = new RuntimeException("both metadata & exception are nulls")
            promise.failure(failure)
          }
        }
      }
      try {
        val result = self.send(jRecord, callback)
        if (result.isDone) {
          val jMetadata = result.get()
          val metadata = jMetadata.asScala
          Future.successful(metadata)
        } else {
          promise.future
        }
      } catch {
        case NonFatal(failure: ExecutionException) => Future.failed(failure.getCause)
        case NonFatal(failure)                     => Future.failed(failure)
      }
    }
  }


  implicit class JRecordMetadataOps(val self: RecordMetadataJ) extends AnyVal {

    def asScala: RecordMetadata = RecordMetadata(
      topicPartition = TopicPartition(self.topic, self.partition()),
      timestamp = (self.timestamp noneIf RecordBatch.NO_TIMESTAMP).map(Instant.ofEpochMilli),
      offset = self.offset noneIf ProduceResponse.INVALID_OFFSET,
      keySerializedSize = self.serializedKeySize noneIf -1,
      valueSerializedSize = self.serializedValueSize noneIf -1
    )
  }


  implicit class RecordMetadataOps(val self: RecordMetadata) extends AnyVal {

    def asJava: RecordMetadataJ = {
      new RecordMetadataJ(
        self.topicPartition.asJava,
        0,
        self.offset getOrElse ProduceResponse.INVALID_OFFSET,
        self.timestamp.fold(RecordBatch.NO_TIMESTAMP)(_.toEpochMilli),
        null,
        self.keySerializedSize getOrElse -1,
        self.valueSerializedSize getOrElse -1)
    }
  }
}