package com.evolutiongaming.skafka.producer

import java.time.Instant

import cats.implicits._
import com.evolutiongaming.catshelper.{ApplicativeThrowable, MonadThrowable}
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import org.apache.kafka.clients.producer.{ProducerRecord => ProducerRecordJ, RecordMetadata => RecordMetadataJ}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.ProduceResponse

import scala.jdk.CollectionConverters._

object ProducerConverters {

  implicit class ProducerRecordOps[K, V](val self: ProducerRecord[K, V]) extends AnyVal {

    def asJava: ProducerRecordJ[K, V] = {
      new ProducerRecordJ[K, V](
        self.topic,
        self.partition.fold[java.lang.Integer](null) { a => java.lang.Integer.valueOf(a.value) },
        self.timestamp.fold[java.lang.Long](null) { timestamp => timestamp.toEpochMilli },
        self.key.getOrElse(null.asInstanceOf[K]),
        self.value.getOrElse(null.asInstanceOf[V]),
        self.headers.map { _.asJava }.asJava
      )
    }
  }

  implicit class JProducerRecordOps[K, V](val self: ProducerRecordJ[K, V]) extends AnyVal {

    def asScala[F[_]: ApplicativeThrowable]: F[ProducerRecord[K, V]] = {

      Option(self.partition)
        .traverse { partition => Partition.of[F](partition.intValue()) }
        .map { partition =>
          ProducerRecord(
            topic     = self.topic,
            value     = Option(self.value),
            key       = Option(self.key),
            partition = partition,
            timestamp = Option(self.timestamp) map { Instant.ofEpochMilli(_) },
            headers   = self.headers.asScala.map { _.asScala }.toList
          )
        }
    }
  }

  implicit class JRecordMetadataOps(val self: RecordMetadataJ) extends AnyVal {

    def asScala[F[_]: MonadThrowable]: F[RecordMetadata] = {
      for {
        partition <- Partition.of[F](self.partition())
        offset    <- (self.offset noneIf ProduceResponse.INVALID_OFFSET).traverse { Offset.of[F] }
      } yield {
        RecordMetadata(
          topicPartition      = TopicPartition(self.topic, partition),
          timestamp           = (self.timestamp noneIf RecordBatch.NO_TIMESTAMP).map(Instant.ofEpochMilli),
          offset              = offset,
          keySerializedSize   = self.serializedKeySize noneIf -1,
          valueSerializedSize = self.serializedValueSize noneIf -1
        )
      }
    }
  }

  implicit class RecordMetadataOps(val self: RecordMetadata) extends AnyVal {

    def asJava: RecordMetadataJ = {
      new RecordMetadataJ(
        self.topicPartition.asJava,
        0,
        self.offset.fold(ProduceResponse.INVALID_OFFSET) { _.value },
        self.timestamp.fold(RecordBatch.NO_TIMESTAMP)(_.toEpochMilli),
        null,
        self.keySerializedSize getOrElse -1,
        self.valueSerializedSize getOrElse -1
      )
    }
  }
}
