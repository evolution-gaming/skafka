package com.evolutiongaming.skafka.consumer

import java.lang.Long as LongJ
import java.time.Instant
import java.util.{Collection as CollectionJ, Map as MapJ}

import cats.Order
import cats.data.{NonEmptyList as Nel, NonEmptyMap as Nem, NonEmptySet as Nes}
import cats.implicits.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.skafka.Converters.*
import com.evolutiongaming.skafka.*
import org.apache.kafka.clients.consumer.{
  Consumer as ConsumerJ,
  ConsumerGroupMetadata as ConsumerGroupMetadataJ,
  ConsumerRebalanceListener as RebalanceListenerJ,
  ConsumerRecord as ConsumerRecordJ,
  ConsumerRecords as ConsumerRecordsJ,
  OffsetAndMetadata as OffsetAndMetadataJ,
  OffsetAndTimestamp as OffsetAndTimestampJ
}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType as TimestampTypeJ
import org.apache.kafka.common.TopicPartition as TopicPartitionJ

import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters.*
import scala.util.Try

object ConsumerConverters {

  implicit class OffsetAndTimestampJOps(val self: OffsetAndTimestampJ) extends AnyVal {

    def asScala[F[_]: ApplicativeThrowable]: F[OffsetAndTimestamp] = {
      for {
        offset <- Offset.of[F](self.offset())
      } yield {
        OffsetAndTimestamp(offset = offset, timestamp = Instant.ofEpochMilli(self.timestamp()))
      }
    }
  }

  implicit class OffsetAndTimestampOps(val self: OffsetAndTimestamp) extends AnyVal {

    def asJava: OffsetAndTimestampJ = new OffsetAndTimestampJ(self.offset.value, self.timestamp.toEpochMilli)
  }

  implicit class RebalanceListener1Ops[F[_]](val self: RebalanceListener1[F]) extends AnyVal {

    def asJava(consumer: ConsumerJ[?, ?])(implicit toTry: ToTry[F]): RebalanceListenerJ = {

      val rebalanceConsumer = RebalanceConsumer(consumer)

      def onPartitions(
        partitions: CollectionJ[TopicPartitionJ],
        call: Nes[TopicPartition] => RebalanceCallback[F, Unit]
      ): Unit = {
        // If you're thinking about deriving ToTry timeout based on ConsumerConfig.maxPollInterval
        // please have a look on https://github.com/evolution-gaming/skafka/issues/125
        partitions
          .asScala
          .toList
          // Note: `traverse(_.asScala[F])` may cause StackOverflowError later, when executed
          // synchronously with `.toTry`. Traversing into `Try` directly avoids this.
          .traverse { _.asScala[Try] }
          .flatMap { topicPartitions =>
            topicPartitions
              .toSortedSet
              .toNes
              .traverse_ { partitions => call(partitions).run(rebalanceConsumer) }
          }
          // If we fail to make a `call(..).run(..)`, fail right now on the consumer thread.
          .get
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

    def asScala[F[_]: MonadThrowable]: F[ConsumerRecord[K, V]] = {
      MonadThrowable[F].catchNonFatal { consumerRecordOf(self) }
    }
  }

  /** Throws [[SkafkaError]] if the partition or the offset is out of range.
    */
  private def consumerRecordOf[K, V](recordJ: ConsumerRecordJ[K, V]): ConsumerRecord[K, V] = {

    val timestampAndType = {
      def some(timestampType: TimestampType): Some[TimestampAndType] = {
        Some(TimestampAndType(Instant.ofEpochMilli(recordJ.timestamp()), timestampType))
      }

      recordJ.timestampType() match {
        case TimestampTypeJ.NO_TIMESTAMP_TYPE => None
        case TimestampTypeJ.CREATE_TIME       => some(TimestampType.create)
        case TimestampTypeJ.LOG_APPEND_TIME   => some(TimestampType.append)
      }
    }

    def withSize[A](value: A, size: Int): Option[WithSize[A]] = {
      Option(value).map { value => WithSize(value, size) }
    }

    ConsumerRecord(
      topicPartition   = TopicPartition(recordJ.topic(), Partition.unsafeOf(recordJ.partition())),
      offset           = Offset.unsafeOf(recordJ.offset()),
      timestampAndType = timestampAndType,
      key              = withSize(recordJ.key(), recordJ.serializedKeySize),
      value            = withSize(recordJ.value(), recordJ.serializedValueSize()),
      headers          = recordJ.headers().asScala.map(_.asScala).toList
    )
  }

  /** Converts the records without an `F` step for each record.
    *
    * `KafkaConsumer` returns the records already grouped by partition, in offset order, so its lists are used as they
    * are. `ConsumerRecords` can also be built by hand with any lists, so the records are grouped by their own partition
    * as soon as one of them is listed under another partition.
    */
  private def consumerRecordsOf[K, V](recordsJ: ConsumerRecordsJ[K, V]): ConsumerRecords[K, V] = {
    if (recordsJ.isEmpty) {
      ConsumerRecords.empty
    } else {
      val listed = listedRecordsOf(recordsJ)
      if (listed.forall { case (partitionJ, records) => isListedUnderOwnPartition(partitionJ, records) }) {
        consumerRecordsByListedPartition(listed)
      } else {
        consumerRecordsByOwnPartition(listed)
      }
    }
  }

  /** Each record is converted once, and the result keeps the partition order and record order of `recordsJ`, as
    * `recordsJ.iterator()` has them.
    */
  private def listedRecordsOf[K, V](
    recordsJ: ConsumerRecordsJ[K, V]
  ): List[(TopicPartitionJ, List[ConsumerRecord[K, V]])] = {
    recordsJ.partitions().asScala.toList.map { partitionJ =>
      (partitionJ, recordsJ.records(partitionJ).asScala.toList.map { consumerRecordOf(_) })
    }
  }

  private def isListedUnderOwnPartition[K, V](
    partitionJ: TopicPartitionJ,
    records: List[ConsumerRecord[K, V]]
  ): Boolean = {
    records.forall { record =>
      record.topicPartition.partition.value == partitionJ.partition() && record.topicPartition.topic == partitionJ
        .topic()
    }
  }

  private def consumerRecordsByListedPartition[K, V](
    listed: List[(TopicPartitionJ, List[ConsumerRecord[K, V]])]
  ): ConsumerRecords[K, V] = {
    val builder = SortedMap.newBuilder[TopicPartition, Nel[ConsumerRecord[K, V]]](Order[TopicPartition].toOrdering)
    listed.foreach {
      case (_, records) =>
        Nel.fromList(records).foreach { records => builder.addOne((records.head.topicPartition, records)) }
    }
    ConsumerRecords(builder.result())
  }

  private def consumerRecordsByOwnPartition[K, V](
    listed: List[(TopicPartitionJ, List[ConsumerRecord[K, V]])]
  ): ConsumerRecords[K, V] = {
    Nel.fromList(listed.flatMap { case (_, records) => records }).fold(ConsumerRecords.empty[K, V]) { records =>
      ConsumerRecords(records.groupBy { _.topicPartition })
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
        self.key.map(_.serializedSize) getOrElse -1,
        self.value.map(_.serializedSize) getOrElse -1,
        self.key.map(_.value) getOrElse null.asInstanceOf[K],
        self.value.map(_.value) getOrElse null.asInstanceOf[V],
        new RecordHeaders(headers),
        Option.empty[Integer].toOptional
      )
    }
  }

  implicit class ConsumerRecordsJOps[K, V](val self: ConsumerRecordsJ[K, V]) extends AnyVal {

    def asScala[F[_]: MonadThrowable]: F[ConsumerRecords[K, V]] = {
      // `MonadThrowable` has no `delay`: the `flatMap` keeps the conversion from running before `F` runs
      MonadThrowable[F].unit.flatMap { _ => MonadThrowable[F].catchNonFatal(consumerRecordsOf(self)) }
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
        groupId         = self.groupId(),
        generationId    = self.generationId(),
        memberId        = self.memberId(),
        groupInstanceId = self.groupInstanceId().toOption
      )
    }
  }

  implicit class ConsumerGroupMetadataOps(val self: ConsumerGroupMetadata) extends AnyVal {

    def asJava: ConsumerGroupMetadataJ = {
      new ConsumerGroupMetadataJ(self.groupId, self.generationId, self.memberId, self.groupInstanceId.toOptional)
    }
  }

  def offsetsAndTimestampsMapF[F[_]: MonadThrowable](
    mapJ: MapJ[TopicPartitionJ, OffsetAndTimestampJ]
  ): F[Map[TopicPartition, Option[OffsetAndTimestamp]]] = {
    mapJ.asScalaMap(_.asScala[F], v => Option(v).traverse { _.asScala[F] }, keepNullValues = true)
  }

  def timestampsToSearchJ(nem: Nem[TopicPartition, Instant]): MapJ[TopicPartitionJ, LongJ] = {
    nem.toSortedMap.asJavaMap(_.asJava, _.toEpochMilli)
  }
}
