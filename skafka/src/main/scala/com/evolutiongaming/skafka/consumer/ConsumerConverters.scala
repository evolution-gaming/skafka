package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.Instant
import java.util.{Collection => CollectionJ, Map => MapJ}

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.effect.Concurrent
import cats.implicits._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.catshelper._
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import org.apache.kafka.clients.consumer.{Consumer => ConsumerJ, ConsumerGroupMetadata => ConsumerGroupMetadataJ, ConsumerRebalanceListener => RebalanceListenerJ, ConsumerRecord => ConsumerRecordJ, ConsumerRecords => ConsumerRecordsJ, OffsetAndMetadata => OffsetAndMetadataJ, OffsetAndTimestamp => OffsetAndTimestampJ}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.{TimestampType => TimestampTypeJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

import scala.jdk.CollectionConverters._

object ConsumerConverters {

  implicit class OffsetAndTimestampJOps(val self: OffsetAndTimestampJ) extends AnyVal {

    def asScala[F[_] : ApplicativeThrowable]: F[OffsetAndTimestamp] = {
      for {
        offset <- Offset.of[F](self.offset())
      } yield {
        OffsetAndTimestamp(
          offset = offset,
          timestamp = Instant.ofEpochMilli(self.timestamp()))
      }
    }
  }


  implicit class OffsetAndTimestampOps(val self: OffsetAndTimestamp) extends AnyVal {

    def asJava: OffsetAndTimestampJ = new OffsetAndTimestampJ(self.offset.value, self.timestamp.toEpochMilli)
  }


  implicit class RebalanceListenerOps[F[_]](val self: RebalanceListener[F]) extends AnyVal {

    def asJava(
      serialListeners: SerialListeners[F])(implicit
      F: Concurrent[F],
      toTry: ToTry[F],
      toFuture: ToFuture[F]
    ): RebalanceListenerJ = {

      def onPartitions(
        partitions: CollectionJ[TopicPartitionJ],
        call: Nes[TopicPartition] => F[Unit]
      ) = {
        serialListeners
          .listener {
            partitions
              .asScala
              .toList
              .traverse { _.asScala[F] }
              .flatMap { partitions =>
                partitions
                  .toSortedSet
                  .toNes
                  .foldMapM(call)
              }
          }
          .toTry
          .get
          .toFuture
        ()
      }

      new RebalanceListenerJ {

        def onPartitionsAssigned(partitions: CollectionJ[TopicPartitionJ]) = {
          onPartitions(partitions, self.onPartitionsAssigned)
        }

        def onPartitionsRevoked(partitions: CollectionJ[TopicPartitionJ]) = {
          onPartitions(partitions, self.onPartitionsRevoked)
        }

        override def onPartitionsLost(partitions: CollectionJ[TopicPartitionJ]) = {
          onPartitions(partitions, self.onPartitionsLost)
        }
      }
    }
  }

  implicit class RebalanceListener1Ops[F[_]](val self: RebalanceListener1[F]) extends AnyVal {

    def asJava(consumer: ConsumerJ[_, _])(implicit
      F: Concurrent[F],
      toTry: ToTry[F],
    ): RebalanceListenerJ = {

      def onPartitions(
        partitions: CollectionJ[TopicPartitionJ],
        call: Nes[TopicPartition] => RebalanceCallback[F, Unit]
      ): Unit = {
        // If you're thinking about deriving ToTry timeout based on ConsumerConfig.maxPollInterval
        // please have a look on https://github.com/evolution-gaming/skafka/issues/125
        val result = partitions
          .asScala
          .toList
          .traverse { _.asScala[F] }
          .map { partitions =>
            partitions
              .toSortedSet
              .toNes
          }
          .toTry
          .flatMap {
            _.foldMapM { partitions => RebalanceCallback.run(call(partitions), RebalanceConsumerJ(consumer)) }
          }
        result.fold(throw _, _ => ())
      }

      new RebalanceListenerJ {

        def onPartitionsAssigned(partitions: CollectionJ[TopicPartitionJ]): Unit = {
          onPartitions(partitions, self.onPartitionsAssigned)
        }

        def onPartitionsRevoked(partitions: CollectionJ[TopicPartitionJ]): Unit = {
          onPartitions(partitions, self.onPartitionsRevoked)
        }

        override def onPartitionsLost(partitions: CollectionJ[TopicPartitionJ]): Unit = {
          onPartitions(partitions, self.onPartitionsLost)
        }
      }
    }
  }


  implicit class ConsumerRecordJOps[K, V](val self: ConsumerRecordJ[K, V]) extends AnyVal {

    def asScala[F[_] : MonadThrowable]: F[ConsumerRecord[K, V]] = {

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

      for {
        partition <- Partition.of[F](self.partition())
        offset    <- Offset.of[F](self.offset())
      } yield {
        ConsumerRecord(
          topicPartition = TopicPartition(self.topic(), partition),
          offset = offset,
          timestampAndType = timestampAndType,
          key = withSize(self.key(), self.serializedKeySize),
          value = withSize(self.value(), self.serializedValueSize()),
          headers = headers)
      }
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
        self.topicPartition.partition.value,
        self.offset.value,
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

    def asScala[F[_] : MonadThrowable]: F[ConsumerRecords[K, V]] = {
      self
        .iterator()
        .asScala
        .toList
        .traverse { _.asScala[F] }
        .map { records =>
          Nel
            .fromList(records)
            .map { records => ConsumerRecords(records.groupBy { _.topicPartition }) }
            .getOrElse(ConsumerRecords.empty)
        }
    }
  }


  implicit class TopicPartitionToOffsetMetadataMapOps(val m: Map[TopicPartition, OffsetAndMetadata]) extends AnyVal {
    def deepAsJava: MapJ[TopicPartitionJ, OffsetAndMetadataJ] = m.map {
      case (tp, om) => (tp.asJava, om.asJava)
    }.asJava
  }


  implicit class ConsumerGroupMetadataJOps(val self: ConsumerGroupMetadataJ) extends AnyVal {

    def asScala: ConsumerGroupMetadata = {
      ConsumerGroupMetadata(
        groupId = self.groupId(),
        generationId = self.generationId(),
        memberId = self.memberId(),
        groupInstanceId = self.groupInstanceId().toOption)
    }
  }


  implicit class ConsumerGroupMetadataOps(val self: ConsumerGroupMetadata) extends AnyVal {

    def asJava: ConsumerGroupMetadataJ = {
      new ConsumerGroupMetadataJ(
        self.groupId,
        self.generationId,
        self.memberId,
        self.groupInstanceId.toOptional)
    }
  }

  def offsetsAndTimestampsMapF[F[_] : MonadThrowable](mapJ: MapJ[TopicPartitionJ, OffsetAndTimestampJ]): F[Map[TopicPartition, Option[OffsetAndTimestamp]]] = {
    mapJ.asScalaMap(_.asScala[F], v => Option(v).traverse { _.asScala[F] })
  }

  def timestampsToSearchJ(nem: Nem[TopicPartition, Instant]): MapJ[TopicPartitionJ, LongJ] = {
    nem.toSortedMap.asJavaMap(_.asJava, _.toEpochMilli)
  }
}
