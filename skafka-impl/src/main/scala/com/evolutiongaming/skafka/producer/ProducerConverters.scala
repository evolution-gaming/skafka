package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.producer.Producer._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConverters._

object ProducerConverters {

  implicit class RecordOps[K, V](val self: Record[K, V]) extends AnyVal {

    def asJava: ProducerRecord[K, V] = {
      new ProducerRecord[K, V](
        self.topic,
        self.partition.fold[java.lang.Integer](null) { java.lang.Integer.valueOf },
        self.timestamp.fold[java.lang.Long](null) { java.lang.Long.valueOf },
        self.key.getOrElse(null.asInstanceOf[K]),
        self.value,
        self.headers.map { _.asJava }.asJava)
    }
  }


  implicit class JRecordOps[K, V](val self: ProducerRecord[K, V]) extends AnyVal {

    def asScala: Record[K, V] = {
      Record(
        topic = self.topic,
        value = self.value,
        key = Option(self.key),
        partition = Option(self.partition) map { _.intValue() },
        timestamp = Option(self.timestamp) map { _.longValue() },
        headers = self.headers.asScala.map { _.asScala }.toList
      )
    }
  }
}