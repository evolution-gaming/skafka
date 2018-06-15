package com.evolutiongaming.skafka.producer


import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import com.evolutiongaming.concurrent.sequentially.SequentiallyHandler
import com.evolutiongaming.skafka.Bytes
import com.evolutiongaming.skafka.producer.ProducerConverters._
import org.apache.kafka.clients.producer.{Producer => JProducer}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object CreateProducer {

  def apply(
    producer: JProducer[Bytes, Bytes],
    sequentially: SequentiallyHandler[Any],
    ecBlocking: ExecutionContext,
    random: Random = new Random)
    (implicit ec: ExecutionContext): Producer = new Producer {

    def doApply[K, V](record: ProducerRecord[K, V])(implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {
      val keySequentially: Any = record.key getOrElse random.nextInt()
      val result = sequentially.handler(keySequentially) {
        Future {
          val keyBytes = record.key.map(keyToBytes.apply)
          val valueBytes = valueToBytes(record.value)
          val recordBytes = record.copy(value = valueBytes, key = keyBytes)
          () =>
            asyncBlocking {
              producer.sendAsScala(recordBytes)
            }
        }
      }

      result.flatMap(identity)
    }

    def flush(): Future[Unit] = {
      asyncBlocking {
        producer.flush()
      }
    }

    def close(): Unit = producer.close()

    def closeAsync(timeout: FiniteDuration): Future[Unit] = {
      asyncBlocking {
        producer.close(timeout.length, timeout.unit)
      }
    }

    private def asyncBlocking[T](f: => T): Future[T] = Future(f)(ecBlocking)
  }

  def apply(configs: ProducerConfig, ecBlocking: ExecutionContext)(implicit system: ActorSystem): Producer = {
    implicit val materializer = CreateMaterializer(configs)
    val sequentially = SequentiallyHandler[Any](overflowStrategy = OverflowStrategy.dropNew)
    val jProducer = CreateJProducer(configs)
    apply(jProducer, sequentially, ecBlocking)(system.dispatcher)
  }
}
