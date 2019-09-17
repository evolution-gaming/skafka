package com.evolutiongaming.skafka
package producer

import cats.effect._
import cats.implicits._
import cats.{Applicative, Functor, MonadError, ~>}
import com.evolutiongaming.catshelper.{FromFuture, Log, MonadThrowable}
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.smetrics.MeasureDuration
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, ProducerRecord => ProducerRecordJ, RecordMetadata => RecordMetadataJ}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionException, Promise}

/**
  * See [[org.apache.kafka.clients.producer.Producer]]
  */
trait Producer[F[_]] {

  def initTransactions: F[Unit]

  def beginTransaction: F[Unit]

  def sendOffsetsToTransaction(
    offsets: Map[TopicPartition, OffsetAndMetadata],
    consumerGroupId: String
  ): F[Unit]

  def commitTransaction: F[Unit]

  def abortTransaction: F[Unit]

  /**
    * @return Outer F[_] is about batching, inner F[_] is about sending
    */
  def send[K, V](
    record: ProducerRecord[K, V])(implicit
    toBytesK: ToBytes[F, K],
    toBytesV: ToBytes[F, V]
  ): F[F[RecordMetadata]]

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

      def send[K, V](record: ProducerRecord[K, V])(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]) = {
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


      def send[K, V](
        record: ProducerRecord[K, V])(implicit
        toBytesK: ToBytes[F, K],
        toBytesV: ToBytes[F, V]
      ) = {

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
                val exception = SkafkaError("both metadata & exception are nulls")
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

        def toBytesError(error: Throwable) = {
          SkafkaError(s"toBytes failed for $record", error).raiseError[F, ProducerRecord[Bytes, Bytes]]
        }

        for {
          bytes  <- record.toBytes.handleErrorWith(toBytesError)
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


  def apply[F[_] : MeasureDuration, E](
    producer: Producer[F],
    metrics: ProducerMetrics[F])(implicit
    F: MonadError[F, E],
  ): Producer[F] = {

    new Producer[F] {

      val initTransactions = {
        for {
          d <- MeasureDuration[F].start
          r <- producer.initTransactions.attempt
          d <- d
          _ <- metrics.initTransactions(d)
          r <- r.liftTo[F]
        } yield r
      }

      val beginTransaction = {
        for {
          r <- producer.beginTransaction
          _ <- metrics.beginTransaction
        } yield r
      }

      def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        for {
          d <- MeasureDuration[F].start
          r <- producer.sendOffsetsToTransaction(offsets, consumerGroupId).attempt
          d <- d
          _ <- metrics.sendOffsetsToTransaction(d)
          r <- r.liftTo[F]
        } yield r
      }

      val commitTransaction = {
        for {
          d <- MeasureDuration[F].start
          r <- producer.commitTransaction.attempt
          d <- d
          _ <- metrics.commitTransaction(d)
          r <- r.liftTo[F]
        } yield r
      }

      val abortTransaction = {
        for {
          d <- MeasureDuration[F].start
          r <- producer.abortTransaction.attempt
          d <- d
          _ <- metrics.abortTransaction(d)
          r <- r.liftTo[F]
        } yield r
      }

      def send[K, V](
        record: ProducerRecord[K, V])(implicit
        toBytesK: ToBytes[F, K],
        toBytesV: ToBytes[F, V]
      ) = {
        val topic = record.topic
        for {
          d <- MeasureDuration[F].start
          r <- producer.send(record).attempt
          d <- d
          _ <- metrics.block(topic, d)
          _ <- r match {
            case Right(_) => ().pure[F]
            case Left(_)  => metrics.failure(topic, d)
          }
          r <- r.liftTo[F]
        } yield for {
          d <- MeasureDuration[F].start
          r <- r.attempt
          d <- d
          _ <- r match {
            case Right(r) => metrics.send(topic, d, r.valueSerializedSize getOrElse 0)
            case Left(_)  => metrics.failure(topic, d)
          }
          r <- r.liftTo[F]
        } yield r
      }

      def partitions(topic: Topic) = {
        for {
          d <- MeasureDuration[F].start
          r <- producer.partitions(topic).attempt
          d <- d
          _ <- metrics.partitions(topic, d)
          r <- r.liftTo[F]
        } yield r
      }

      val flush = {
        for {
          d <- MeasureDuration[F].start
          r <- producer.flush.attempt
          d <- d
          _ <- metrics.flush(d)
          r <- r.liftTo[F]
        } yield r
      }
    }
  }


  implicit class ProducerOps[F[_]](val self: Producer[F]) extends AnyVal {

    def withLogging(log: Log[F])(implicit F: MonadThrowable[F]): Producer[F] = {
      ProducerLogging(self, log)
    }


    def withMetrics[E](
      metrics: ProducerMetrics[F])(implicit
      F: MonadError[F, E],
      measureDuration: MeasureDuration[F]
    ): Producer[F] = {
      Producer(self, metrics)
    }


    def mapK[G[_] : Functor](fg: F ~> G, gf: G ~> F): Producer[G] = new Producer[G] {

      def initTransactions = fg(self.initTransactions)

      def beginTransaction = fg(self.beginTransaction)

      def sendOffsetsToTransaction(offsets: Map[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        fg(self.sendOffsetsToTransaction(offsets, consumerGroupId))
      }

      def commitTransaction = fg(self.commitTransaction)

      def abortTransaction = fg(self.abortTransaction)

      def send[K, V](
        record: ProducerRecord[K, V])(implicit
        toBytesK: ToBytes[G, K],
        toBytesV: ToBytes[G, V]
      ) = {
        for {
          a <- fg(self.send(record)(toBytesK.mapK(gf), toBytesV.mapK(gf)))
        } yield {
          fg(a)
        }
      }

      def partitions(topic: Topic) = fg(self.partitions(topic))

      def flush = fg(self.flush)
    }


    def toSend: Send[F] = Send(self)


    def sendNoKey[V](
      record: ProducerRecord[Nothing, V])(implicit
      F: Applicative[F],
      toBytes: ToBytes[F, V]
    ): F[F[RecordMetadata]] = {
      self.send(record)(ToBytes.empty, toBytes)
    }


    def sendNoVal[K](
      record: ProducerRecord[K, Nothing])(implicit
      F: Applicative[F],
      toBytes: ToBytes[F, K]
    ): F[F[RecordMetadata]] = {
      self.send(record)(toBytes, ToBytes.empty)
    }


    def sendEmpty(
      record: ProducerRecord[Nothing, Nothing])(implicit
      F: Applicative[F]
    ): F[F[RecordMetadata]] = {
      self.send(record)(ToBytes.empty, ToBytes.empty)
    }
  }


  trait Send[F[_]] {

    def apply[K, V](
      record: ProducerRecord[K, V])(implicit
      toBytesK: ToBytes[F, K],
      toBytesV: ToBytes[F, V]
    ): F[F[RecordMetadata]]
  }

  object Send {

    def empty[F[_] : Applicative]: Send[F] = apply(Producer.empty)


    def apply[F[_]](producer: Producer[F]): Send[F] = new Send[F] {

      def apply[K, V](
        record: ProducerRecord[K, V])(implicit
        toBytesK: ToBytes[F, K],
        toBytesV: ToBytes[F, V]
      ) = {
        producer.send(record)
      }
    }


    implicit class SendOps[F[_]](val self: Send[F]) extends AnyVal {

      def mapK[G[_] : Functor](fg: F ~> G, gf: G ~> F): Send[G] = new Send[G] {

        def apply[K, V](
          record: ProducerRecord[K, V])(implicit
          toBytesK: ToBytes[G, K],
          toBytesV: ToBytes[G, V]
        ) = {
          for {
            a <- fg(self(record)(toBytesK.mapK(gf), toBytesV.mapK(gf)))
          } yield {
            fg(a)
          }
        }
      }


      def noKey[V](record: ProducerRecord[Nothing, V])(implicit
        F: Applicative[F],
        toBytes: ToBytes[F, V]
      ): F[F[RecordMetadata]] = {
        self.apply(record)(ToBytes.empty, toBytes)
      }


      def noVal[K](
        record: ProducerRecord[K, Nothing])(implicit
        F: Applicative[F],
        toBytes: ToBytes[F, K]
      ): F[F[RecordMetadata]] = {
        self.apply(record)(toBytes, ToBytes.empty)
      }


      def empty(
        record: ProducerRecord[Nothing, Nothing])(implicit
        F: Applicative[F]
      ): F[F[RecordMetadata]] = {
        self.apply(record)(ToBytes.empty, ToBytes.empty)
      }
    }
  }
}


