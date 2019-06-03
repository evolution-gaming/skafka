package com.evolutiongaming.skafka
package producer

import akka.actor.ActorSystem
import akka.stream.{Materializer, OverflowStrategy}
import cats.Applicative
import cats.effect.{Sync, ContextShift}
import cats.implicits._
import com.evolutiongaming.catshelper.FromFuture
import com.evolutiongaming.concurrent.sequentially.SequentiallyAsync
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import org.apache.kafka.clients.producer.{Producer => ProducerJ}

import scala.collection.JavaConverters._
import scala.compat.Platform
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

trait Producer[F[_]] {

  def initTransactions: F[Unit]

  def beginTransaction: F[Unit]

  def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String): F[Unit]

  def commitTransaction: F[Unit]

  def abortTransaction: F[Unit]

  def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata]


  final def sendNoKey[V: ToBytes](record: ProducerRecord[Nothing, V]): F[RecordMetadata] = {
    send(record)(ToBytes.empty, ToBytes[V])
  }

  final def sendNoVal[K: ToBytes](record: ProducerRecord[K, Nothing]): F[RecordMetadata] = {
    send(record)(ToBytes[K], ToBytes.empty)
  }

  final def sendEmpty(record: ProducerRecord[Nothing, Nothing]): F[RecordMetadata] = {
    send(record)(ToBytes.empty, ToBytes.empty)
  }

  def partitions(topic: Topic): F[List[PartitionInfo]]

  def flush: F[Unit]

  def close: F[Unit]

  def close(timeout: FiniteDuration): F[Unit]
}

object Producer {
  def empty[F[_] : Applicative] = new Producer[F] {
    private val ap = Applicative[F]
    private val empty: F[Unit] = ap.pure(())

    override val initTransactions: F[Unit] = empty

    override val beginTransaction: F[Unit] = empty

    override def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      consumerGroupId: String): F[Unit] = empty

    override val commitTransaction: F[Unit] = empty

    override val abortTransaction: F[Unit] = empty

    override def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata] =
      ap.pure {
        val partition = record.partition getOrElse Partition.Min
        val topicPartition = TopicPartition(record.topic, partition)
        val metadata = RecordMetadata(topicPartition, record.timestamp)
        metadata
      }

    override def partitions(topic: Topic): F[List[PartitionInfo]] = ap.pure(Nil)

    override val flush: F[Unit] = empty

    override val close: F[Unit] = empty

    override def close(timeout: FiniteDuration): F[Unit] = empty
  }

  abstract class DefaultProducer[F[_] : Sync : ContextShift : FromFuture : Blocking](
    producer: ProducerJ[Bytes, Bytes]) extends Producer[F] {

    val blocking = implicitly[Blocking[F]]

    val initTransactions = blocking {
      producer.initTransactions
    }

    val beginTransaction = blocking {
      producer.beginTransaction
    }

    def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) =
      blocking {
        val offsetsJ = offsets.asJavaMap(_.asJava, _.asJava)
        producer.sendOffsetsToTransaction(offsetsJ, consumerGroupId)
      }

    val commitTransaction = blocking {
      producer.commitTransaction
    }

    val abortTransaction = blocking {
      producer.abortTransaction
    }

    def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata] =
      for {
        recordBytes <- Sync[F].delay(record.toBytes)
        result <- blocking.future(producer.sendAsScala(recordBytes))
      } yield result


    def partitions(topic: Topic) = blocking {
      producer.partitionsFor(topic).asScala.map(_.asScala).toList
    }

    val flush = blocking {
      producer.flush
    }

    def close(timeout: FiniteDuration) = blocking {
      producer.close(timeout.length, timeout.unit)
    }

    val close = blocking {
      producer.close
    }
  }

  def apply[F[_] : Producer]: Producer[F] = implicitly[Producer[F]]

  def apply[F[_] : Sync : ContextShift : FromFuture](
    producer: ProducerJ[Bytes, Bytes],
    blockingEc: ExecutionContext): Producer[F] = {
    implicit val b = Blocking(blockingEc)
    new DefaultProducer[F](producer) {}
  }


  def apply[F[_] : Sync : ContextShift : FromFuture](
    producer: ProducerJ[Bytes, Bytes],
    sequentially: SequentiallyAsync[Int],
    blockingEc: ExecutionContext,
    random: Random = new Random): Producer[F] = {
    implicit val b = Blocking[F](blockingEc)
    new DefaultProducer[F](producer) {
      val async = Sync[F]
      import async.delay
      import b._

      override def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata] =
        for {
          key <- delay(record.key.fold(random.nextInt())(_.hashCode()))
          recordBytes <- delay(record.toBytes)
          metadata <- future(sequentially.async(key)(producer.sendAsScala(recordBytes)))
        } yield metadata
    }
  }

  def apply[F[_] : Sync](
    producer: Producer[F],
    metrics: Metrics[F]): Producer[F] = {
    val async = Sync[F]
    import async.delay

    def measured[A](action: Producer[F] => F[A])(metric: Metrics[F] => Long => F[Unit]): F[A] =
      for {
        time <- delay(Platform.currentTime)
        result <- action(producer)
        latency <- delay(Platform.currentTime - time)
        _ <- metric(metrics)(latency)
      } yield {
        result
      }

    def measuredParam[A](action: Producer[F] => F[A])(metric: (Metrics[F], A) => Long => F[Unit]): F[A] =
      for {
        time <- delay(Platform.currentTime)
        result <- action(producer)
        latency <- delay(Platform.currentTime - time)
        _ <- metric(metrics, result)(latency)
      } yield {
        result
      }

    new Producer[F] {
      override val initTransactions: F[Unit] = measured(_.initTransactions)(_.initTransactions)

      override val beginTransaction: F[Unit] = metrics.beginTransaction *> producer.beginTransaction

      override def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String): F[Unit] =
        measured(_.sendOffsetsToTransaction(offsets, consumerGroupId))(_.sendOffsetsToTransaction)

      override val commitTransaction: F[Unit] = measured(_.commitTransaction)(_.commitTransaction)

      override val abortTransaction: F[Unit] = measured(_.abortTransaction)(_.abortTransaction)

      override def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata] =
        measuredParam(_.send(record))((m, r) => m.send(record.topic, _, r.valueSerializedSize.getOrElse(0)))

      override def partitions(topic: Topic): F[List[PartitionInfo]] =
        measured(_.partitions(topic))(m => m.partitions(topic, _))

      override val flush: F[Unit] = measured(_.flush)(_.flush)

      override val close: F[Unit] = measured(_.close)(_.close)

      override def close(timeout: FiniteDuration): F[Unit] = measured(_.close(timeout))(_.close)
    }
  }

  def apply[F[_] : Sync : ContextShift : FromFuture](
    config: ProducerConfig,
    ecBlocking: ExecutionContext,
    system: ActorSystem): Producer[F] = {
    implicit val materializer: Materializer = CreateMaterializer(config)(system)
    val sequentially = SequentiallyAsync[Int](overflowStrategy = OverflowStrategy.dropNew)
    val jProducer = CreateJProducer(config)
    apply(jProducer, sequentially, ecBlocking)
  }

  def apply[F[_] : Sync : ContextShift : FromFuture](
    config: ProducerConfig,
    ecBlocking: ExecutionContext): Producer[F] = {
    val producer = CreateJProducer(config)
    apply(producer, ecBlocking)
  }

  trait Send[F[_]] {

    def apply[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata]

    final def noKey[V: ToBytes](record: ProducerRecord[Nothing, V]): F[RecordMetadata] = {
      apply(record)(ToBytes.empty, ToBytes[V])
    }

    final def noVal[K: ToBytes](record: ProducerRecord[K, Nothing]): F[RecordMetadata] = {
      apply(record)(ToBytes[K], ToBytes.empty)
    }

    final def empty(record: ProducerRecord[Nothing, Nothing]): F[RecordMetadata] = {
      apply(record)(ToBytes.empty, ToBytes.empty)
    }
  }

  object Send {
    def empty[F[_] : Applicative]: Send[F] = apply(Producer.empty)

    def apply[F[_]](producer: Producer[F]): Send[F] = new Send[F] {
      def apply[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]) = {
        producer.send(record)
      }
    }
  }
}


