package com.evolutiongaming.skafka
package producer

import cats.data.{NonEmptyMap => Nem}
import cats.effect._
import cats.effect.concurrent.Deferred
import cats.implicits._
import cats.{Applicative, Functor, MonadError, ~>}
import com.evolutiongaming.catshelper.{Blocking, Log, MonadThrowable}
import com.evolutiongaming.catshelper.Blocking.implicits._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.smetrics.MeasureDuration
import org.apache.kafka.clients.producer.{
  Callback,
  Producer => ProducerJ,
  ProducerRecord => ProducerRecordJ,
  RecordMetadata => RecordMetadataJ
}

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, ExecutionException}

/** See [[org.apache.kafka.clients.producer.Producer]]
  */
trait Producer[F[_]] {

  def initTransactions: F[Unit]

  def beginTransaction: F[Unit]

  def sendOffsetsToTransaction(
    offsets: Nem[TopicPartition, OffsetAndMetadata],
    consumerGroupId: String
  ): F[Unit]

  def commitTransaction: F[Unit]

  def abortTransaction: F[Unit]

  /** @return
    *   Outer F[_] is about sending event (including batching if required), inner F[_] is about waiting and getting the
    *   result of the send operation.
    */
  def send[K, V](
    record: ProducerRecord[K, V]
  )(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]): F[F[RecordMetadata]]

  def partitions(topic: Topic): F[List[PartitionInfo]]

  def flush: F[Unit]
}

object Producer {

  def apply[F[_]](implicit F: Producer[F]): Producer[F] = F

  private sealed abstract class Empty

  def empty[F[_]: Applicative]: Producer[F] = {

    val empty = ().pure[F]

    new Empty with Producer[F] {

      val initTransactions = empty

      val beginTransaction = empty

      def sendOffsetsToTransaction(
        offsets: Nem[TopicPartition, OffsetAndMetadata],
        consumerGroupId: String
      ) = empty

      val commitTransaction = empty

      val abortTransaction = empty

      def send[K, V](record: ProducerRecord[K, V])(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]) = {
        val partition      = record.partition getOrElse Partition.min
        val topicPartition = TopicPartition(record.topic, partition)
        val metadata       = RecordMetadata(topicPartition, record.timestamp)
        metadata.pure[F].pure[F]
      }

      def partitions(topic: Topic) = List.empty[PartitionInfo].pure[F]

      val flush = empty
    }
  }

  def of[F[_]: Effect: ContextShift](
    config: ProducerConfig,
    executorBlocking: ExecutionContext
  ): Resource[F, Producer[F]] = {
    implicit val blocking = Blocking.fromExecutionContext(executorBlocking)
    val producer          = CreateProducerJ(config)
    fromProducerJ(producer)
  }

  private sealed abstract class Main

  def fromProducerJ[F[_]: Effect: Blocking](producer: F[ProducerJ[Bytes, Bytes]]): Resource[F, Producer[F]] = {

    def blocking[A](f: => A) = Sync[F].delay(f).blocking

    def apply(producer: ProducerJ[Bytes, Bytes]) = {
      new Main with Producer[F] {

        val initTransactions = {
          blocking { producer.initTransactions() }
        }

        val beginTransaction = {
          Sync[F].delay { producer.beginTransaction() }
        }

        def sendOffsetsToTransaction(offsets: Nem[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
          val offsetsJ = offsets
            .toSortedMap
            .asJavaMap(_.asJava, _.asJava)
          blocking { producer.sendOffsetsToTransaction(offsetsJ, consumerGroupId) }
        }

        val commitTransaction = {
          blocking { producer.commitTransaction() }
        }

        val abortTransaction = {
          blocking { producer.abortTransaction() }
        }

        def send[K, V](record: ProducerRecord[K, V])(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]) = {

          def executionException[A]: PartialFunction[Throwable, F[A]] = {
            case failure: ExecutionException => failure.getCause.raiseError[F, A]
          }

          def unsafeRunSync[A](fa: F[A]): A = Effect[F].toIO(fa).unsafeRunSync()

          def block(record: ProducerRecordJ[Bytes, Bytes]) = {

            def callbackOf(deferred: Deferred[F, Either[Throwable, RecordMetadataJ]]): Callback = {
              (metadata: RecordMetadataJ, exception: Exception) =>
                if (exception != null) {
                  unsafeRunSync(deferred.complete(exception.asLeft))
                } else if (metadata != null) {
                  unsafeRunSync(deferred.complete(metadata.asRight))
                } else {
                  val exception = SkafkaError("both metadata & exception are nulls")
                  unsafeRunSync(deferred.complete(exception.asLeft))
                }
            }

            Sync[F].uncancelable {
              for {
                deferred <- Deferred.uncancelable[F, Either[Throwable, RecordMetadataJ]]
                callback  = callbackOf(deferred)
                _        <- blocking { producer.send(record, callback) }.recoverWith(executionException)
              } yield {
                val result = deferred.get.flatMap(Sync[F].fromEither)
                result.recoverWith(executionException)
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
            result <- result.asScala[F]
          } yield result
        }

        def partitions(topic: Topic) = {
          for {
            result <- blocking { producer.partitionsFor(topic) }
            result <- result.asScala.toList.traverse { _.asScala[F] }
          } yield result
        }

        val flush = {
          blocking { producer.flush() }
        }
      }
    }

    for {
      producerJ <- producer.blocking.toResource
      producer   = apply(producerJ)
      close      = blocking { producerJ.close() }
      flush      = producer.flush.attempt
      _         <- Resource.release { flush *> close }
    } yield producer
  }

  private sealed abstract class WithMetrics

  def apply[F[_]: MeasureDuration, E](producer: Producer[F], metrics: ProducerMetrics[F])(
    implicit F: MonadError[F, E],
  ): Producer[F] = {

    new WithMetrics with Producer[F] {

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

      def sendOffsetsToTransaction(offsets: Nem[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
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

      def send[K, V](record: ProducerRecord[K, V])(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]) = {
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

  private sealed abstract class MapK

  implicit class ProducerOps[F[_]](val self: Producer[F]) extends AnyVal {

    def withLogging(log: Log[F])(implicit F: MonadThrowable[F], measureDuration: MeasureDuration[F]): Producer[F] = {
      ProducerLogging(self, log)
    }

    def withMetrics[E](
      metrics: ProducerMetrics[F]
    )(implicit F: MonadError[F, E], measureDuration: MeasureDuration[F]): Producer[F] = {
      Producer(self, metrics)
    }

    def mapK[G[_]: Functor](fg: F ~> G, gf: G ~> F): Producer[G] = new MapK with Producer[G] {

      def initTransactions = fg(self.initTransactions)

      def beginTransaction = fg(self.beginTransaction)

      def sendOffsetsToTransaction(offsets: Nem[TopicPartition, OffsetAndMetadata], consumerGroupId: String) = {
        fg(self.sendOffsetsToTransaction(offsets, consumerGroupId))
      }

      def commitTransaction = fg(self.commitTransaction)

      def abortTransaction = fg(self.abortTransaction)

      def send[K, V](record: ProducerRecord[K, V])(implicit toBytesK: ToBytes[G, K], toBytesV: ToBytes[G, V]) = {
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
      record: ProducerRecord[Nothing, V]
    )(implicit F: Applicative[F], toBytes: ToBytes[F, V]): F[F[RecordMetadata]] = {
      self.send(record)(ToBytes.empty, toBytes)
    }

    def sendNoVal[K](
      record: ProducerRecord[K, Nothing]
    )(implicit F: Applicative[F], toBytes: ToBytes[F, K]): F[F[RecordMetadata]] = {
      self.send(record)(toBytes, ToBytes.empty)
    }

    def sendEmpty(record: ProducerRecord[Nothing, Nothing])(implicit F: Applicative[F]): F[F[RecordMetadata]] = {
      self.send(record)(ToBytes.empty, ToBytes.empty)
    }
  }

  trait Send[F[_]] {

    def apply[K, V](
      record: ProducerRecord[K, V]
    )(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]): F[F[RecordMetadata]]
  }

  object Send {

    def empty[F[_]: Applicative]: Send[F] = apply(Producer.empty)

    def apply[F[_]](producer: Producer[F]): Send[F] = new Send[F] {

      def apply[K, V](record: ProducerRecord[K, V])(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]) = {
        producer.send(record)
      }
    }

    implicit class SendOps[F[_]](val self: Send[F]) extends AnyVal {

      def mapK[G[_]: Functor](fg: F ~> G, gf: G ~> F): Send[G] = new Send[G] {

        def apply[K, V](record: ProducerRecord[K, V])(implicit toBytesK: ToBytes[G, K], toBytesV: ToBytes[G, V]) = {
          for {
            a <- fg(self(record)(toBytesK.mapK(gf), toBytesV.mapK(gf)))
          } yield {
            fg(a)
          }
        }
      }

      def noKey[V](
        record: ProducerRecord[Nothing, V]
      )(implicit F: Applicative[F], toBytes: ToBytes[F, V]): F[F[RecordMetadata]] = {
        self.apply(record)(ToBytes.empty, toBytes)
      }

      def noVal[K](
        record: ProducerRecord[K, Nothing]
      )(implicit F: Applicative[F], toBytes: ToBytes[F, K]): F[F[RecordMetadata]] = {
        self.apply(record)(toBytes, ToBytes.empty)
      }

      def empty(record: ProducerRecord[Nothing, Nothing])(implicit F: Applicative[F]): F[F[RecordMetadata]] = {
        self.apply(record)(ToBytes.empty, ToBytes.empty)
      }
    }
  }
}
