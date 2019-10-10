package com.evolutiongaming.skafka.consumer

import java.time.Instant
import java.util.{Collection => CollectionJ, Map => MapJ}

import cats.data.{NonEmptyList => Nel}
import cats.effect.Concurrent
import cats.effect.concurrent.Semaphore
import cats.implicits._
import com.evolutiongaming.catshelper.EffectHelper._
import com.evolutiongaming.catshelper.ToFuture
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.{OffsetAndMetadata, TimestampAndType, TimestampType, TopicPartition}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener => RebalanceListenerJ, ConsumerRecord => ConsumerRecordJ, ConsumerRecords => ConsumerRecordsJ, OffsetAndMetadata => OffsetAndMetadataJ, OffsetAndTimestamp => OffsetAndTimestampJ}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.{TimestampType => TimestampTypeJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

import scala.jdk.CollectionConverters._

object ConsumerConverters {

  implicit class OffsetAndTimestampJOps(val self: OffsetAndTimestampJ) extends AnyVal {
    def asScala: OffsetAndTimestamp = OffsetAndTimestamp(
      offset = self.offset(),
      timestamp = Instant.ofEpochMilli(self.timestamp()))
  }


  implicit class OffsetAndTimestampOps(val self: OffsetAndTimestamp) extends AnyVal {
    def asJava: OffsetAndTimestampJ = new OffsetAndTimestampJ(self.offset, self.timestamp.toEpochMilli)
  }


  implicit class RebalanceListenerOps[F[_]](val self: RebalanceListener[F]) extends AnyVal {

    def asJava(implicit F: Concurrent[F], toFuture: ToFuture[F]): F[RebalanceListenerJ] = {
      for {
        semaphore <- Semaphore[F](1)
      } yield {

        def onPartitions(partitions: CollectionJ[TopicPartitionJ])(f: Nel[TopicPartition] => F[Unit]) = {
          val partitionsS = partitions.asScala.map(_.asScala)
          Nel.fromList(partitionsS.toList).foreach { partitions =>
            semaphore.withPermit { f(partitions) }.toFuture
          }
        }

        new RebalanceListenerJ {

          def onPartitionsAssigned(partitions: CollectionJ[TopicPartitionJ]) = {
            onPartitions(partitions)(self.onPartitionsAssigned)
          }

          def onPartitionsRevoked(partitions: CollectionJ[TopicPartitionJ]) = {
            onPartitions(partitions)(self.onPartitionsRevoked)
          }
        }
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

      def withSize[A](value: A, size: Int) = {
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
        recordsJ    = self.records(partitionJ)
        partition   = partitionJ.asScala
        records     = recordsJ.asScala.map(_.asScala).toList
        records    <- Nel.fromList(records)
      } yield {
        (partition, records)
      }
      ConsumerRecords(records.toMap)
    }
  }

  implicit class TopicPartitionToOffsetMetadataMapOps(val m: Map[TopicPartition, OffsetAndMetadata]) extends AnyVal {
    def deepAsJava: MapJ[TopicPartitionJ, OffsetAndMetadataJ] = m.map {
      case (tp, om) => (tp.asJava, om.asJava)
    }.asJava
  }
}
