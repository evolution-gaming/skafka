package com.evolutiongaming.skafka
package producer

import cats.Applicative
import cats.effect.{Async, Clock, ContextShift, Sync}
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, ProducerRecord => ProducerRecordJ, RecordMetadata => RecordMetadataJ}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, ExecutionException}
import scala.util.control.NoStackTrace


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

  def apply[F[_]](implicit F: Producer[F]): Producer[F] = F
  
  
  def empty[F[_] : Applicative]: Producer[F] = {
    
    val empty = ().pure[F]

    new Producer[F] {

      val initTransactions = empty

      val beginTransaction = empty

      def sendOffsetsToTransaction(
        offsets: Map[TopicPartition, OffsetAndMetadata],
        consumerGroupId: String
      ) = empty

      val commitTransaction = empty

      val abortTransaction = empty

      def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]) = {
        val partition = record.partition getOrElse Partition.Min
        val topicPartition = TopicPartition(record.topic, partition)
        val metadata = RecordMetadata(topicPartition, record.timestamp)
        metadata.pure[F]
      }

      def partitions(topic: Topic) = List.empty[PartitionInfo].pure[F]

      val flush = empty

      val close = empty

      def close(timeout: FiniteDuration) = empty
    }
  }


  def apply[F[_] : Async : ContextShift](
    producer: ProducerJ[Bytes, Bytes],
    executorBlocking: ExecutionContext
  ): Producer[F] = {

    val blocking = Blocking(executorBlocking)

    apply(producer, blocking)
  }

  
  def apply[F[_] : Async : ContextShift](
    producer: ProducerJ[Bytes, Bytes],
    blocking: Blocking[F]
  ): Producer[F] = {
    
    new Producer[F] {

      val initTransactions = {
        blocking { producer.initTransactions() }
      }

      val beginTransaction = {
        Sync[F].delay { producer.beginTransaction() }
      }

      def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        val offsetsJ = offsets.asJavaMap(_.asJava, _.asJava)
        blocking { producer.sendOffsetsToTransaction(offsetsJ, consumerGroupId) }
      }

      val commitTransaction = {
        blocking { producer.commitTransaction() }
      }

      val abortTransaction = {
        blocking { producer.abortTransaction() }
      }


      def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]) = {

        def send(record: ProducerRecordJ[Bytes, Bytes]) = {
          Async[F].asyncF[RecordMetadataJ] { f =>
            val callback = new Callback {
              def onCompletion(metadata: RecordMetadataJ, exception: Exception) = {
                if (exception != null) {
                  f(exception.asLeft)
                } else if (metadata != null) {
                  f(metadata.asRight)
                } else {
                  val exception = new RuntimeException("both metadata & exception are nulls") with NoStackTrace
                  f(exception.asLeft)
                }
              }
            }

            blocking { producer.send(record, callback) }
              .void
              .recoverWith { case failure: ExecutionException => failure.getCause.raiseError[F, Unit] }
          }
        }

        for {
          recordBytes <- Sync[F].delay { record.toBytes }
          recordJ      = recordBytes.asJava
          result      <- send(recordJ)
          _           <- ContextShift[F].shift
        } yield {
          result.asScala
        }
      }


      def partitions(topic: Topic) = {
        for {
          result <- blocking { producer.partitionsFor(topic) }
        } yield {
          result.asScala.map(_.asScala).toList
        }
      }

      val flush = {
        blocking { producer.flush() }
      }

      def close(timeout: FiniteDuration) = {
        blocking { producer.close(timeout.length, timeout.unit) }
      }

      val close = {
        blocking { producer.close() }
      }
    }
  }


  def apply[F[_] : Sync : Clock](
    producer: Producer[F],
    metrics: Metrics[F]
  ): Producer[F] = {
    
    def latency[A](fa: F[A]) = {
      for {
        start   <- Clock[F].millis
        result  <- fa
        end     <- Clock[F].millis
        latency  = end - start
      } yield {
        (result, latency)
      }
    }

    new Producer[F] {
      
      val initTransactions = {
        for {
          rl     <- latency { producer.initTransactions.attempt }
          (r, l)  = rl
          _      <- metrics.initTransactions(l)
          r      <- r.raiseOrPure[F]
        } yield r
      }

      val beginTransaction = {
        for {
          r  <- producer.beginTransaction
          _  <- metrics.beginTransaction
        } yield r
      }

      def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        for {
          rl     <- latency { producer.sendOffsetsToTransaction(offsets, consumerGroupId).attempt }
          (r, l)  = rl
          _      <- metrics.sendOffsetsToTransaction(l)
          r      <- r.raiseOrPure[F]
        } yield r
      }

      val commitTransaction = {
        for {
          rl     <- latency { producer.commitTransaction.attempt }
          (r, l)  = rl
          _      <- metrics.commitTransaction(l)
          r      <- r.raiseOrPure[F]
        } yield r
      }

      val abortTransaction = {
        for {
          rl     <- latency { producer.abortTransaction.attempt }
          (r, l)  = rl
          _      <- metrics.abortTransaction(l)
          r      <- r.raiseOrPure[F]
        } yield r
      }

      def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]) = {
        for {
          rl     <- latency { producer.send(record).attempt }
          (r, l)  = rl
          _      <- r match {
            case Right(r) => metrics.send(record.topic, l, r.valueSerializedSize getOrElse 0)
            case Left(_)  => metrics.failure(record.topic, l)
          }
          r      <- r.raiseOrPure[F]
        } yield r
      }

      def partitions(topic: Topic) = {
        for {
          rl     <- latency { producer.partitions(topic).attempt }
          (r, l)  = rl
          _      <- metrics.partitions(topic, l)
          r      <- r.raiseOrPure[F]
        } yield r
      }

      val flush = {
        for {
          rl     <- latency { producer.flush.attempt }
          (r, l)  = rl
          _      <- metrics.flush(l)
          r      <- r.raiseOrPure[F]
        } yield r
      }

      val close = {
        for {
          rl     <- latency { producer.close.attempt }
          (r, l)  = rl
          _      <- metrics.close(l)
          r      <- r.raiseOrPure[F]
        } yield r
      }

      def close(timeout: FiniteDuration) = {
        for {
          rl     <- latency { producer.close(timeout).attempt }
          (r, l)  = rl
          _      <- metrics.close(l)
          r      <- r.raiseOrPure[F]
        } yield r
      }
    }
  }


  def apply[F[_] : Async : ContextShift](
    config: ProducerConfig,
    executorBlocking: ExecutionContext
  ): Producer[F] = {

    val producer = CreateJProducer(config)
    
    apply(producer, executorBlocking)
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


