package com.evolutiongaming.skafka

import com.evolutiongaming.skafka.producer.Producer.RecordMetadata
import org.apache.kafka.clients.producer.{RecordMetadata => JRecordMetadata}
import org.apache.kafka.common.header.{Header => JHeader}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.{TopicPartition => JTopicPartition}

private[skafka] object Converters {

  implicit class HeaderOps(val self: Header) extends AnyVal {

    def asJava: JHeader = new JHeader {
      def value = self.value
      def key = self.key
    }
  }

  implicit class JHeaderOps(val self: JHeader) extends AnyVal {
    def asScala: Header = Header(self.key(), self.value())
  }


  implicit class JRecordMetadataOps(val self: JRecordMetadata) extends AnyVal {

    def asScala: RecordMetadata = RecordMetadata(
      topic = self.topic,
      partition = self.partition,
      timestamp = self.timestamp noneIf RecordBatch.NO_TIMESTAMP,
      offset = self.offset noneIf ProduceResponse.INVALID_OFFSET,
      serializedKeySize = self.serializedKeySize zeroIf -1,
      serializedValueSize = self.serializedValueSize zeroIf -1
    )
  }

  implicit class RecordMetadataOps(val self: RecordMetadata) extends AnyVal {
    def asJava: JRecordMetadata = {
      val jTopicPartition = new JTopicPartition(self.topic, self.partition)
      new JRecordMetadata(
        jTopicPartition,
        0,
        self.offset getOrElse ProduceResponse.INVALID_OFFSET,
        self.timestamp getOrElse RecordBatch.NO_TIMESTAMP,
        null,
        self.serializedKeySize,
        self.serializedValueSize
      )
    }
  }


  implicit class IntOps(val self: Int) extends AnyVal {

    def zeroIf(x: Int): Int = if (self == x) 0 else self
  }


  implicit class LongOps(val self: Long) extends AnyVal {

    def noneIf(x: Long): Option[Long] = if (self == x) None else Some(self)
  }
}
