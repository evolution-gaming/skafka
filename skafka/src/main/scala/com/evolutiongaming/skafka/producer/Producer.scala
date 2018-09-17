package com.evolutiongaming.skafka.producer

import akka.actor.ActorSystem
import akka.stream.{Materializer, OverflowStrategy}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.concurrent.sequentially.SequentiallyAsync
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import org.apache.kafka.clients.producer.{Producer => JProducer}

import scala.compat.Platform
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

trait Producer {

  def flush(): Future[Unit]

  def close(): Future[Unit]

  def send[K, V](record: ProducerRecord[K, V])
    (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata]

  def close(timeout: FiniteDuration): Future[Unit]


  final def sendNoKey[V](record: ProducerRecord[Nothing, V])(implicit toBytes: ToBytes[V]): Future[RecordMetadata] = {
    send[Nothing, V](record)(toBytes, ToBytes.empty)
  }

  final def sendNoVal[K](record: ProducerRecord[K, Nothing])(implicit toBytes: ToBytes[K]): Future[RecordMetadata] = {
    send[K, Nothing](record)(ToBytes.empty, toBytes)
  }

  final def sendEmpty(record: ProducerRecord[Nothing, Nothing]): Future[RecordMetadata] = {
    send[Nothing, Nothing](record)(ToBytes.empty, ToBytes.empty)
  }
}

object Producer {

  val Empty: Producer = new Producer {

    def send[K, V](record: ProducerRecord[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

      val partition = record.partition getOrElse Partition.Min
      val topicPartition = TopicPartition(record.topic, partition)
      val metadata = RecordMetadata(topicPartition, record.timestamp)
      metadata.future
    }

    def flush() = Future.unit

    def close() = Future.unit

    def close(timeout: FiniteDuration) = Future.unit
  }


  def apply(producer: JProducer[Bytes, Bytes], ecBlocking: ExecutionContext): Producer = {

    def blocking[T](f: => T): Future[T] = Future(f)(ecBlocking)

    new Producer {

      def send[K, V](record: ProducerRecord[K, V])(implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {
        val recordBytes = record.toBytes
        blocking {
          producer.sendAsScala(recordBytes)
        }
      }.flatten

      def flush(): Future[Unit] = {
        blocking {
          producer.flush()
        }
      }

      def close(timeout: FiniteDuration): Future[Unit] = {
        blocking {
          producer.close(timeout.length, timeout.unit)
        }
      }

      def close(): Future[Unit] = {
        blocking {
          producer.close()
        }
      }
    }
  }

  def apply(
    producer: JProducer[Bytes, Bytes],
    sequentially: SequentiallyAsync[Int],
    ecBlocking: ExecutionContext,
    random: Random = new Random): Producer = {

    def blocking[T](f: => T) = Future(f)(ecBlocking)

    new Producer {

      def send[K, V](record: ProducerRecord[K, V])(implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {
        val key = record.key.fold(random.nextInt())(_.hashCode())
        val recordBytes = record.toBytes
        val future = sequentially.async(key) {
          blocking {
            producer.sendAsScala(recordBytes)
          }
        }
        future.flatten
      }

      def flush(): Future[Unit] = {
        blocking {
          producer.flush()
        }
      }

      def close(timeout: FiniteDuration): Future[Unit] = {
        blocking {
          producer.close(timeout.length, timeout.unit)
        }
      }

      def close(): Future[Unit] = {
        blocking {
          producer.close()
        }
      }
    }
  }

  def apply(config: ProducerConfig, ecBlocking: ExecutionContext, system: ActorSystem): Producer = {
    implicit val materializer: Materializer = CreateMaterializer(config)(system)
    val sequentially = SequentiallyAsync[Int](overflowStrategy = OverflowStrategy.dropNew)
    val jProducer = CreateJProducer(config)
    apply(jProducer, sequentially, ecBlocking)
  }

  def apply(config: ProducerConfig, ecBlocking: ExecutionContext): Producer = {
    val producer = CreateJProducer(config)
    apply(producer, ecBlocking)
  }

  def apply(producer: Producer, metrics: Metrics): Producer = {
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

    new Producer {

      def send[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {

        val start = Platform.currentTime
        val result = producer.send(record)(valueToBytes, keyToBytes)
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


  trait Send {

    def apply[K, V](record: ProducerRecord[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata]

    final def noKey[V](record: ProducerRecord[Nothing, V])(implicit toBytes: ToBytes[V]): Future[RecordMetadata] = {
      apply[Nothing, V](record)(toBytes, ToBytes.empty)
    }

    final def noVal[K](record: ProducerRecord[K, Nothing])(implicit toBytes: ToBytes[K]): Future[RecordMetadata] = {
      apply[K, Nothing](record)(ToBytes.empty, toBytes)
    }

    final def empty(record: ProducerRecord[Nothing, Nothing]): Future[RecordMetadata] = {
      apply[Nothing, Nothing](record)(ToBytes.empty, ToBytes.empty)
    }
  }

  object Send {
    val Empty: Send = apply(Producer.Empty)

    def apply(producer: Producer): Send = new Send {
      def apply[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {

        producer.send(record)(valueToBytes, keyToBytes)
      }
    }
  }


  trait Metrics {

    def send(topic: Topic, latency: Long, bytes: Int): Unit

    def failure(topic: Topic, latency: Long): Unit

    def flush(latency: Long): Unit

    def close(latency: Long): Unit
  }

  object Metrics {

    val Empty: Metrics = new Metrics {

      def send(topic: Topic, latency: Offset, bytes: Partition) = {}

      def failure(topic: Topic, latency: Offset) = {}

      def flush(latency: Offset) = {}

      def close(latency: Offset) = {}
    }
  }
}