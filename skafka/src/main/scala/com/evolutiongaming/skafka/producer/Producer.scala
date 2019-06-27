package com.evolutiongaming.skafka
package producer

import cats.effect._
import cats.implicits._
import cats.{Applicative, FlatMap, ~>}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{FromFuture, Log}
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, ProducerRecord => ProducerRecordJ, RecordMetadata => RecordMetadataJ}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionException, Promise}
import scala.util.control.NoStackTrace

/**
  * See [[org.apache.kafka.clients.producer.Producer]]
  */
trait Producer[F[_]] {

  def initTransactions: F[Unit]

  def beginTransaction: F[Unit]

  def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String): F[Unit]

  def commitTransaction: F[Unit]

  def abortTransaction: F[Unit]

  /**
    * @return Outer F[_] is about batching, inner F[_] is about sending
    */
  def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): F[F[RecordMetadata]]


  final def sendNoKey[V: ToBytes](record: ProducerRecord[Nothing, V]): F[F[RecordMetadata]] = {
    send(record)(ToBytes.empty, ToBytes[V])
  }

  final def sendNoVal[K: ToBytes](record: ProducerRecord[K, Nothing]): F[F[RecordMetadata]] = {
    send(record)(ToBytes[K], ToBytes.empty)
  }

  final def sendEmpty(record: ProducerRecord[Nothing, Nothing]): F[F[RecordMetadata]] = {
    send(record)(ToBytes.empty, ToBytes.empty)
  }

  def partitions(topic: Topic): F[List[PartitionInfo]]

  def flush: F[Unit]
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
        metadata.pure[F].pure[F]
      }

      def partitions(topic: Topic) = List.empty[PartitionInfo].pure[F]

      val flush = empty
    }
  }


  def of[F[_] : Sync : ContextShift : FromFuture](
    config: ProducerConfig,
    executorBlocking: ExecutionContext
  ): Resource[F, Producer[F]] = {

    val blocking = Blocking(executorBlocking)

    val result = for {
      producerJ <- CreateProducerJ(config, blocking)
      producer   = apply(producerJ, blocking)
      close      = blocking { producerJ.close() }
    } yield {
      val release = for {
        _ <- producer.flush.attempt
        a <- close
      } yield a
      (producer, release)
    }
    Resource(result)
  }


  def apply[F[_] : Sync : FromFuture](
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

        def executionException[A]: PartialFunction[Throwable, F[A]] = {
          case failure: ExecutionException => failure.getCause.raiseError[F, A]
        }

        def block(record: ProducerRecordJ[Bytes, Bytes]) = {

          def callbackOf(promise: Promise[RecordMetadataJ]) = new Callback {
            def onCompletion(metadata: RecordMetadataJ, exception: Exception) = {
              if (exception != null) {
                promise.failure(exception)
              } else if (metadata != null) {
                promise.success(metadata)
              } else {
                val exception = new RuntimeException("both metadata & exception are nulls") with NoStackTrace
                promise.failure(exception)
              }
            }
          }

          Sync[F].uncancelable {
            for {
              promise  <- Sync[F].delay { Promise[RecordMetadataJ]() }
              callback  = callbackOf(promise)
              _        <- blocking { producer.send(record, callback) }.recoverWith(executionException)
            } yield {
              FromFuture[F].apply(promise.future).recoverWith(executionException)
            }
          }
        }

        for {
          bytes  <- Sync[F].delay { record.toBytes }
          bytesJ  = bytes.asJava
          result <- block(bytesJ)
        } yield for {
          result <- result
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
    }
  }


  def apply[F[_] : Sync : Clock](
    producer: Producer[F],
    metrics: ProducerMetrics[F]
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
        val topic = record.topic
        for {
          rl     <- latency { producer.send(record).attempt }
          (r, l)  = rl
          _ <- metrics.block(topic, l)
          _ <- r  match {
            case Right(_) => ().pure[F]
            case Left(_)  => metrics.failure(topic, l)
          }
          r <- r.raiseOrPure[F]
        } yield for {
          rl <- latency { r.attempt }
          (r, l) = rl
          _ <- r match {
            case Right(r) => metrics.send(topic, l, r.valueSerializedSize getOrElse 0)
            case Left(_)  => metrics.failure(topic, l)
          }
          r <- r.raiseOrPure[F]
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
    }
  }


  implicit class ProducerOps[F[_]](val self: Producer[F]) extends AnyVal {

    def withLogging(log: Log[F])(implicit F: ProducerLogging.MonadThrowable[F]): Producer[F] = {
      ProducerLogging(self, log)
    }

    def withMetrics(metrics: ProducerMetrics[F])(implicit F: Sync[F], clock: Clock[F]): Producer[F] = {
      Producer(self, metrics)
    }

    def mapK[G[_] : FlatMap](f: F ~> G): Producer[G] = new Producer[G] {

      def initTransactions = f(self.initTransactions)

      def beginTransaction = f(self.beginTransaction)

      def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        f(self.sendOffsetsToTransaction(offsets, consumerGroupId))
      }

      def commitTransaction = f(self.commitTransaction)

      def abortTransaction = f(self.abortTransaction)

      def send[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]) = {
        for {
          a <- f(self.send(record))
        } yield {
          f(a)
        }
      }

      def partitions(topic: Topic) = f(self.partitions(topic))

      def flush = f(self.flush)
    }
  }


  trait Send[F[_]] {

    def apply[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]): F[F[RecordMetadata]]

    final def noKey[V: ToBytes](record: ProducerRecord[Nothing, V]): F[F[RecordMetadata]] = {
      apply(record)(ToBytes.empty, ToBytes[V])
    }

    final def noVal[K: ToBytes](record: ProducerRecord[K, Nothing]): F[F[RecordMetadata]] = {
      apply(record)(ToBytes[K], ToBytes.empty)
    }

    final def empty(record: ProducerRecord[Nothing, Nothing]): F[F[RecordMetadata]] = {
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


    implicit class SendOps[F[_]](val self: Send[F]) extends AnyVal {

      def mapK[G[_] : FlatMap](f: F ~> G): Send[G] = new Send[G] {

        def apply[K: ToBytes, V: ToBytes](record: ProducerRecord[K, V]) = {
          for {
            a <- f(self(record))
          } yield {
            f(a)
          }
        }
      }
    }
  }
}


