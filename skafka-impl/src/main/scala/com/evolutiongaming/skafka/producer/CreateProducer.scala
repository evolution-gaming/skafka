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

    def blocking[T](f: => T) = Future(f)(ecBlocking)

    new Producer {

      def send[K, V](record: ProducerRecord[K, V])(implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {
        val recordBytes = record.toBytes
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
    producer: JProducer[Bytes, Bytes],
    sequentially: SequentiallyAsync[Int],
    ecBlocking: ExecutionContext,
    random: Random = new Random): Producer = {

    def blocking[T](f: => T) = Future(f)(ecBlocking)

    new Producer {

      def send[K, V](record: ProducerRecord[K, V])(implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {
        val key = record.key.fold(random.nextInt())(_.hashCode())
        val recordBytes = record.toBytes
        val future = sequentially.async(key) {
          blocking {
            producer.sendAsScala(recordBytes)
          }
        }
        future.flatten
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
  }

  def apply(config: ProducerConfig, ecBlocking: ExecutionContext, system: ActorSystem): Producer = {
    implicit val materializer = CreateMaterializer(config)(system)
    val sequentially = SequentiallyAsync[Int](overflowStrategy = OverflowStrategy.dropNew)
    val jProducer = CreateJProducer(config)
    apply(jProducer, sequentially, ecBlocking)
  }

  def apply(config: ProducerConfig, ecBlocking: ExecutionContext): Producer = {
    val producer = CreateJProducer(config)
    apply(producer, ecBlocking)
  }
}
