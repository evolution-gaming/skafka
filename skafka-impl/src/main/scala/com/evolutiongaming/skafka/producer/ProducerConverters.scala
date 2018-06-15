package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.{Bytes, TopicPartition}
import org.apache.kafka.clients.producer.{Callback, Producer => JProducer, ProducerRecord => JProducerRecord, RecordMetadata => JRecordMetadata}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionException, Future, Promise}
import scala.util.control.NonFatal

object ProducerConverters {

  implicit class ProducerRecordOps[K, V](val self: ProducerRecord[K, V]) extends AnyVal {

    def asJava: JProducerRecord[K, V] = {
      new JProducerRecord[K, V](
        self.topic,
        self.partition.fold[java.lang.Integer](null) { java.lang.Integer.valueOf },
        self.timestamp.fold[java.lang.Long](null) { java.lang.Long.valueOf },
        self.key.getOrElse(null.asInstanceOf[K]),
        self.value,
        self.headers.map { _.asJava }.asJava)
    }
  }


  implicit class JProducerRecordOps[K, V](val self: JProducerRecord[K, V]) extends AnyVal {

    def asScala: ProducerRecord[K, V] = {
      ProducerRecord(
        topic = self.topic,
        value = self.value,
        key = Option(self.key),
        partition = Option(self.partition) map { _.intValue() },
        timestamp = Option(self.timestamp) map { _.longValue() },
        headers = self.headers.asScala.map { _.asScala }.toList
      )
    }
  }


  implicit class SerializerOps[T](val self: Serializer[T]) extends AnyVal {

    def asScala(topic: String = ""): ToBytes[T] = new ToBytes[T] {
      def apply(value: T): Bytes = self.serialize(topic, value)
    }
  }


  implicit class JProducerOps[K, V](val self: JProducer[K, V]) extends AnyVal {

    def sendAsScala(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
      val jRecord = record.asJava
      val promise = Promise[RecordMetadata]
      val callback = new Callback {
        def onCompletion(metadata: JRecordMetadata, exception: Exception) = {
          if (metadata == null) promise.failure(exception)
          else promise.success(metadata.asScala)
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


  implicit class JRecordMetadataOps(val self: JRecordMetadata) extends AnyVal {

    def asScala: RecordMetadata = RecordMetadata(
      topicPartition = TopicPartition(self.topic, self.partition()),
      timestamp = self.timestamp noneIf RecordBatch.NO_TIMESTAMP,
      offset = self.offset noneIf ProduceResponse.INVALID_OFFSET,
      serializedKeySize = self.serializedKeySize zeroIf -1,
      serializedValueSize = self.serializedValueSize zeroIf -1
    )
  }


  implicit class RecordMetadataOps(val self: RecordMetadata) extends AnyVal {

    def asJava: JRecordMetadata = {
      new JRecordMetadata(
        self.topicPartition.asJava,
        0,
        self.offset getOrElse ProduceResponse.INVALID_OFFSET,
        self.timestamp getOrElse RecordBatch.NO_TIMESTAMP,
        null,
        self.serializedKeySize,
        self.serializedValueSize
      )
    }
  }
}