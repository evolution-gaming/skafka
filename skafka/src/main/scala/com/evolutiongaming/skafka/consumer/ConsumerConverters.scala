package com.evolutiongaming.skafka.consumer

import java.time.Instant
import java.util.{Collection => CollectionJ, Map => MapJ}

import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.{TimestampAndType, TimestampType, TopicPartition}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener => RebalanceListenerJ, ConsumerRecord => ConsumerRecordJ, ConsumerRecords => ConsumerRecordsJ, OffsetAndMetadata => OffsetAndMetadataJ, OffsetAndTimestamp => OffsetAndTimestampJ, OffsetCommitCallback => CommitCallbackJ}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.{TimestampType => TimestampTypeJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

import scala.collection.JavaConverters._
import scala.collection.immutable.Iterable
import scala.util.{Failure, Success}

object ConsumerConverters {

  implicit class OffsetAndMetadataJOps(val self: OffsetAndMetadataJ) extends AnyVal {
    def asScala: OffsetAndMetadata = OffsetAndMetadata(self.offset(), self.metadata())
  }


  implicit class OffsetAndMetadataOps(val self: OffsetAndMetadata) extends AnyVal {
    def asJava: OffsetAndMetadataJ = new OffsetAndMetadataJ(self.offset, self.metadata)
  }


  implicit class OffsetAndTimestampJOps(val self: OffsetAndTimestampJ) extends AnyVal {
    def asScala: OffsetAndTimestamp = OffsetAndTimestamp(
      offset = self.offset(),
      timestamp = Instant.ofEpochMilli(self.timestamp()))
  }


  implicit class OffsetAndTimestampOps(val self: OffsetAndTimestamp) extends AnyVal {
    def asJava: OffsetAndTimestampJ = new OffsetAndTimestampJ(self.offset, self.timestamp.toEpochMilli)
  }


  implicit class RebalanceListenerOps(val self: RebalanceListener) extends AnyVal {

    def asJava: RebalanceListenerJ = new RebalanceListenerJ {

      def onPartitionsAssigned(partitions: CollectionJ[TopicPartitionJ]) = {
        val partitionsS = partitions.asScala.map(_.asScala).to[Iterable]
        self.onPartitionsAssigned(partitionsS)
      }

      def onPartitionsRevoked(partitions: CollectionJ[TopicPartitionJ]) = {
        val partitionsS = partitions.asScala.map(_.asScala).to[Iterable]
        self.onPartitionsRevoked(partitionsS)
      }
    }
  }


  implicit class RebalanceListenerJOps(val self: RebalanceListenerJ) extends AnyVal {

    def asScala: RebalanceListener = new RebalanceListener {

      def onPartitionsAssigned(partitions: Iterable[TopicPartition]): Unit = {
        val partitionsJ = partitions.map(_.asJava).asJavaCollection
        self.onPartitionsAssigned(partitionsJ)
      }

      def onPartitionsRevoked(partitions: Iterable[TopicPartition]): Unit = {
        val partitionsJ = partitions.map(_.asJava).asJavaCollection
        self.onPartitionsRevoked(partitionsJ)
      }
    }
  }


  implicit class CommitCallbackOps(val self: CommitCallback) extends AnyVal {

    def asJava: CommitCallbackJ = new CommitCallbackJ {

      def onComplete(offsetsJ: MapJ[TopicPartitionJ, OffsetAndMetadataJ], exception: Exception): Unit = {
        val offsets =
          if (exception == null) {
            val offsets = offsetsJ.asScalaMap(_.asScala, _.asScala)
            Success(offsets)
          } else {
            Failure(exception)
          }
        self(offsets)
      }
    }
  }


  implicit class ConsumerRecordJOps[K, V](val self: ConsumerRecordJ[K, V]) extends AnyVal {

    def asScala: ConsumerRecord[K, V] = {

      val headers = self.headers().asScala.map(_.asScala).toList

      val timestampAndType = {
        def some(timestampType: TimestampType) = {
          Some(TimestampAndType(Instant.ofEpochMilli(self.timestamp()), timestampType))
        }

        self.timestampType() match {
          case TimestampTypeJ.NO_TIMESTAMP_TYPE => None
          case TimestampTypeJ.CREATE_TIME       => some(TimestampType.Create)
          case TimestampTypeJ.LOG_APPEND_TIME   => some(TimestampType.Append)
        }
      }

      def withSize[T](value: T, size: Int) = {
        for {
          value <- Option(value)
        } yield WithSize(value, size)
      }

      ConsumerRecord(
        topicPartition = TopicPartition(self.topic(), self.partition()),
        offset = self.offset(),
        timestampAndType = timestampAndType,
        key = withSize(self.key(), self.serializedKeySize),
        value = withSize(self.value(), self.serializedValueSize()),
        headers = headers)
    }
  }

  implicit class ConsumerRecordOps[K, V](val self: ConsumerRecord[K, V]) extends AnyVal {

    def asJava: ConsumerRecordJ[K, V] = {

      val headers = self.headers.map(_.asJava).asJava

      val (timestampType, timestamp) = self.timestampAndType map { timestampAndType =>
        timestampAndType.timestampType match {
          case TimestampType.Create => (TimestampTypeJ.CREATE_TIME, timestampAndType.timestamp.toEpochMilli)
          case TimestampType.Append => (TimestampTypeJ.LOG_APPEND_TIME, timestampAndType.timestamp.toEpochMilli)
        }
      } getOrElse {
        (TimestampTypeJ.NO_TIMESTAMP_TYPE, -1L)
      }


      new ConsumerRecordJ[K, V](
        self.topicPartition.topic,
        self.topicPartition.partition,
        self.offset,
        timestamp,
        timestampType,
        null,
        self.key.map(_.serializedSize) getOrElse -1,
        self.value.map(_.serializedSize) getOrElse -1,
        self.key.map(_.value) getOrElse null.asInstanceOf[K],
        self.value.map(_.value) getOrElse null.asInstanceOf[V],
        new RecordHeaders(headers))
    }
  }


  implicit class ConsumerRecordsJOps[K, V](val self: ConsumerRecordsJ[K, V]) extends AnyVal {

    def asScala: ConsumerRecords[K, V] = {
      val partitions = self.partitions()
      val records = for {
        partitionJ <- partitions.asScala
      } yield {
        val recordsJ = self.records(partitionJ)
        val partition = partitionJ.asScala
        val records = recordsJ.asScala.map(_.asScala).toVector
        (partition, records)
      }
      ConsumerRecords(records.toMap)
    }
  }
}
