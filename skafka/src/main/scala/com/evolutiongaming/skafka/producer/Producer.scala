package com.evolutiongaming.skafka.producer

import akka.actor.ActorSystem
import akka.stream.{Materializer, OverflowStrategy}
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.concurrent.sequentially.SequentiallyAsync
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Bytes, ToBytes, TopicPartition}
import org.apache.kafka.clients.producer.{Producer => JProducer}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

trait Producer {

  def flush(): Future[Unit]

  def close(): Future[Unit]

  def send[K, V](record: ProducerRecord[K, V])
    (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata]

  def close(timeout: FiniteDuration): Future[Unit]


  def sendNoKey[V](record: ProducerRecord[Nothing, V])(implicit toBytes: ToBytes[V]): Future[RecordMetadata] = {
    send[Nothing, V](record)(toBytes, ToBytes.empty)
  }

  def sendNoVal[K](record: ProducerRecord[K, Nothing])(implicit toBytes: ToBytes[K]): Future[RecordMetadata] = {
    send[K, Nothing](record)(ToBytes.empty, toBytes)
  }

  def sendEmpty(record: ProducerRecord[Nothing, Nothing]): Future[RecordMetadata] = {
    send[Nothing, Nothing](record)(ToBytes.empty, ToBytes.empty)
  }
}

object Producer {

  lazy val Empty: Producer = new Producer {

    def send[K, V](record: ProducerRecord[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata] = {

      val partition = record.partition getOrElse 0
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


  object Send {
    val Empty: Send = apply(Producer.Empty)

    def apply(producer: Producer): Send = new Send {
      def apply[K, V](record: ProducerRecord[K, V])
        (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {

        producer.send(record)(valueToBytes, keyToBytes)
      }

      def apply[V](record: ProducerRecord[Nothing, V])
        (implicit valueToBytes: ToBytes[V]) = {

        producer.sendNoKey(record)(valueToBytes)
      }
    }
  }

  trait Send {
    def apply[K, V](record: ProducerRecord[K, V])
      (implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]): Future[RecordMetadata]

    def apply[V](record: ProducerRecord[Nothing, V])
      (implicit valueToBytes: ToBytes[V]): Future[RecordMetadata]
  }
}