package com.evolutiongaming.skafka.producer

import akka.actor.ActorSystem
import akka.stream.{Materializer, OverflowStrategy}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.concurrent.sequentially.SequentiallyAsync
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import org.apache.kafka.clients.producer.{Producer => ProducerJ}

import scala.collection.JavaConverters._
import scala.compat.Platform
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

trait Producer[F[_]] {

  def initTransactions(): F[Unit]

  def beginTransaction(): Unit

  def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String): F[Unit]

  def commitTransaction(): F[Unit]

  def abortTransaction(): F[Unit]

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

  def flush(): F[Unit]

  def close(): F[Unit]

  def close(timeout: FiniteDuration): F[Unit]
}

object Producer {

  val Empty: Producer[Future] = new Producer[Future] {

    def initTransactions() = Future.unit

    def beginTransaction() = {}

    def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = Future.unit

    def commitTransaction() = Future.unit

    def abortTransaction() = Future.unit

    def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): Future[RecordMetadata] = {
      val partition = record.partition getOrElse Partition.Min
      val topicPartition = TopicPartition(record.topic, partition)
      val metadata = RecordMetadata(topicPartition, record.timestamp)
      metadata.future
    }

    def partitions(topic: Topic) = Future.nil

    def flush() = Future.unit

    def close() = Future.unit

    def close(timeout: FiniteDuration) = Future.unit
  }

  def apply(producer: ProducerJ[Bytes, Bytes], ecBlocking: ExecutionContext): Producer[Future] = {
    apply(producer, Blocking.future(ecBlocking))
  }

  def apply(producer: ProducerJ[Bytes, Bytes], blocking: Blocking[Future]): Producer[Future] = new Producer[Future] {

    def initTransactions() = {
      blocking {
        producer.initTransactions()
      }
    }

    def beginTransaction() = {
      producer.beginTransaction()
    }

    def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
      blocking {
        val offsetsJ = offsets.asJavaMap(_.asJava, _.asJava)
        producer.sendOffsetsToTransaction(offsetsJ, consumerGroupId)
      }
    }

    def commitTransaction() = {
      blocking {
        producer.commitTransaction()
      }
    }

    def abortTransaction() = {
      blocking {
        producer.abortTransaction()
      }
    }

    def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]) = {
      val recordBytes = record.toBytes
      blocking {
        producer.sendAsScala(recordBytes)
      }
    }.flatten

    def partitions(topic: Topic) = {
      blocking {
        val result = producer.partitionsFor(topic)
        result.asScala.map(_.asScala).toList
      }
    }

    def flush() = {
      blocking {
        producer.flush()
      }
    }

    def close(timeout: FiniteDuration) = {
      blocking {
        producer.close(timeout.length, timeout.unit)
      }
    }

    def close() = {
      blocking {
        producer.close()
      }
    }
  }

  def apply(
    producer: ProducerJ[Bytes, Bytes],
    sequentially: SequentiallyAsync[Int],
    ecBlocking: ExecutionContext,
    random: Random = new Random): Producer[Future] = {

    val blocking = Blocking.future(ecBlocking)

    val self = apply(producer, blocking)

    new Producer[Future] {

      def initTransactions() = self.initTransactions()

      def beginTransaction() = self.beginTransaction()

      def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        self.sendOffsetsToTransaction(offsets, consumerGroupId)
      }

      def commitTransaction() = self.commitTransaction()

      def abortTransaction() = self.abortTransaction()

      def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): Future[RecordMetadata] = {
        val key = record.key.fold(random.nextInt())(_.hashCode())
        val recordBytes = record.toBytes
        val future = sequentially.async(key) {
          blocking {
            producer.sendAsScala(recordBytes)
          }
        }
        future.flatten
      }

      def partitions(topic: Topic) = self.partitions(topic)

      def flush() = self.flush()

      def close(timeout: FiniteDuration) = self.close(timeout)

      def close() = self.close()
    }
  }

  def apply(config: ProducerConfig, ecBlocking: ExecutionContext, system: ActorSystem): Producer[Future] = {
    implicit val materializer: Materializer = CreateMaterializer(config)(system)
    val sequentially = SequentiallyAsync[Int](overflowStrategy = OverflowStrategy.dropNew)
    val jProducer = CreateJProducer(config)
    apply(jProducer, sequentially, ecBlocking)
  }

  def apply(config: ProducerConfig, ecBlocking: ExecutionContext): Producer[Future] = {
    val producer = CreateJProducer(config)
    apply(producer, ecBlocking)
  }

  def apply(producer: Producer[Future], metrics: Metrics): Producer[Future] = {
    implicit val ec = CurrentThreadExecutionContext

    def latency[T](f: => Future[T]) = {
      val time = Platform.currentTime
      for {
        result <- f
        latency = Platform.currentTime - time
      } yield {
        (result, latency)
      }
    }

    new Producer[Future] {

      def initTransactions() = {
        for {
          tuple <- latency { producer.initTransactions() }
          (result, latency) = tuple
          _ = metrics.initTransactions(latency)
        } yield result
      }

      def beginTransaction() = {
        metrics.beginTransaction()
        producer.beginTransaction()
      }

      def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        for {
          tuple <- latency { producer.sendOffsetsToTransaction(offsets, consumerGroupId) }
          (result, latency) = tuple
          _ = metrics.sendOffsetsToTransaction(latency)
        } yield result
      }

      def commitTransaction() = {
        for {
          tuple <- latency { producer.commitTransaction() }
          (result, latency) = tuple
          _ = metrics.commitTransaction(latency)
        } yield result
      }

      def abortTransaction() = {
        for {
          tuple <- latency { producer.abortTransaction() }
          (result, latency) = tuple
          _ = metrics.abortTransaction(latency)
        } yield result
      }

      def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]) = {
        val start = Platform.currentTime
        val result = producer.send(record)
        result.onComplete { result =>
          val topic = record.topic
          val latency = Platform.currentTime - start
          result match {
            case Success(metadata) =>
              val bytes = metadata.valueSerializedSize.getOrElse(0)
              metrics.send(topic, latency = latency, bytes = bytes)
            case Failure(_)        =>
              metrics.failure(topic, latency)
          }
        }
        result
      }

      def partitions(topic: Topic) = {
        for {
          tuple <- latency { producer.partitions(topic) }
          (result, latency) = tuple
          _ = metrics.partitions(topic, latency)
        } yield result
      }

      def flush() = {
        for {
          tuple <- latency { producer.flush() }
          (result, latency) = tuple
          _ = metrics.flush(latency)
        } yield result
      }

      def close() = {
        for {
          tuple <- latency { producer.close() }
          (result, latency) = tuple
          _ = metrics.close(latency)
        } yield result
      }

      def close(timeout: FiniteDuration) = {
        for {
          tuple <- latency { producer.close(timeout) }
          (result, latency) = tuple
          _ = metrics.close(latency)
        } yield result
      }
    }
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
    val Empty: Send[Future] = apply(Producer.Empty)

    def apply[F[_]](producer: Producer[F]): Send[F] = new Send[F] {
      def apply[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]) = {
        producer.send(record)
      }
    }
  }


  trait Metrics {

    def initTransactions(latency: Long): Unit

    def beginTransaction(): Unit

    def sendOffsetsToTransaction(latency: Long): Unit

    def commitTransaction(latency: Long): Unit

    def abortTransaction(latency: Long): Unit

    def send(topic: Topic, latency: Long, bytes: Int): Unit

    def failure(topic: Topic, latency: Long): Unit

    def partitions(topic: Topic, latency: Long): Unit

    def flush(latency: Long): Unit

    def close(latency: Long): Unit
  }

  object Metrics {

    val Empty: Metrics = new Metrics {

      def initTransactions(latency: Long) = {}

      def beginTransaction() = {}

      def sendOffsetsToTransaction(latency: Long) = {}

      def commitTransaction(latency: Long) = {}

      def abortTransaction(latency: Long) = {}

      def send(topic: Topic, latency: Long, bytes: Int) = {}

      def failure(topic: Topic, latency: Long) = {}

      def partitions(topic: Topic, latency: Long) = {}

      def flush(latency: Long) = {}

      def close(latency: Long) = {}
    }
  }
}