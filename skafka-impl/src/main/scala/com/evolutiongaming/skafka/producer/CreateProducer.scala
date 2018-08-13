package com.evolutiongaming.skafka.producer


import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import com.evolutiongaming.concurrent.sequentially.SequentiallyAsync
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Bytes, ToBytes}
import org.apache.kafka.clients.producer.{Producer => JProducer}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object CreateProducer {

  def apply(producer: JProducer[Bytes, Bytes], ecBlocking: ExecutionContext): Producer = {

    def blocking[T](f: => T): Future[T] = Future(f)(ecBlocking)

    new Producer {

      def doApply[K, V](record: ProducerRecord[K, V])(implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {
        val topic = record.topic
        val keyBytes = record.key.map { key => keyToBytes(key, topic) }
        val valueBytes = valueToBytes(record.value, topic)
        val recordBytes = record.copy(value = valueBytes, key = keyBytes)
        blocking {
          producer.sendAsScala(recordBytes)
        }
      }.flatten

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
  }

  def apply(
    producerJ: JProducer[Bytes, Bytes],
    sequentially: SequentiallyAsync[Int],
    ecBlocking: ExecutionContext,
    random: Random = new Random)
    (implicit ec: ExecutionContext): Producer = {

    val producer = apply(producerJ, ecBlocking)

    apply(producer, sequentially, random)
  }

  def apply(
    producer: Producer,
    sequentially: SequentiallyAsync[Int],
    random: Random)
    (implicit ec: ExecutionContext): Producer = {

    new Producer {

      def doApply[K, V](record: ProducerRecord[K, V])(implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {
        val key = record.key.fold(random.nextInt())(_.hashCode())
        sequentially.async(key) {
          Future {
            val topic = record.topic
            val keyBytes = record.key.map { key => keyToBytes(key, topic) }
            val valueBytes = valueToBytes(record.value, topic)
            val recordBytes = record.copy(value = valueBytes, key = keyBytes)
            producer(recordBytes)
          }.flatten
        }
      }

      def flush() = producer.flush()

      def close(timeout: FiniteDuration) = producer.close(timeout)

      def close() = producer.close()
    }
  }

  def apply(configs: ProducerConfig, ecBlocking: ExecutionContext, system: ActorSystem): Producer = {
    implicit val materializer = CreateMaterializer(configs)(system)
    val sequentially = SequentiallyAsync[Int](overflowStrategy = OverflowStrategy.dropNew)
    val jProducer = CreateJProducer(configs)
    apply(jProducer, sequentially, ecBlocking)(system.dispatcher)
  }

  def apply(configs: ProducerConfig, ecBlocking: ExecutionContext): Producer = {
    val producer = CreateJProducer(configs)
    apply(producer, ecBlocking)
  }
}
