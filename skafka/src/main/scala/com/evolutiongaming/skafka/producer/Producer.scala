package com.evolutiongaming.skafka
package producer

import cats.effect.{Async, Deferred, Resource, Sync}
import cats.effect.implicits._
import cats.implicits._
import cats.{Applicative, Functor, Monad, MonadError, MonadThrow, ~>}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{Blocking, Log, MeasureDuration, ToTry}
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import org.apache.kafka.clients.producer.{
  Callback,
  Producer => ProducerJ,
  ProducerRecord => ProducerRecordJ,
  RecordMetadata => RecordMetadataJ
}

import scala.concurrent.{ExecutionContext, ExecutionException}
import scala.jdk.CollectionConverters._

/**
  * See [[org.apache.kafka.clients.producer.Producer]]
  */
trait Producer[F[_]] {

  def initTransactions: F[Unit]

  def beginTransaction: F[Unit]

  def commitTransaction: F[Unit]

  def abortTransaction: F[Unit]

  /**
    * @return Outer F[_] is about sending event (including batching if required),
    * inner F[_] is about waiting and getting the result of the send operation.
    */
  def send[K, V](
    record: ProducerRecord[K, V]
  )(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]): F[F[RecordMetadata]]

  def partitions(topic: Topic): F[List[PartitionInfo]]

  def flush: F[Unit]

  def clientMetrics: F[Seq[ClientMetric[F]]]
}

object Producer {

  def apply[F[_]](implicit F: Producer[F]): Producer[F] = F

  private sealed abstract class Empty

  def empty[F[_]: Applicative]: Producer[F] = {

    def empty = ().pure[F]

    new Empty with Producer[F] {

      def initTransactions = empty

      def beginTransaction = empty

      def commitTransaction = empty

      def abortTransaction = empty

      def send[K, V](record: ProducerRecord[K, V])(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]) = {
        val partition      = record.partition getOrElse Partition.min
        val topicPartition = TopicPartition(record.topic, partition)
        val metadata       = RecordMetadata(topicPartition, record.timestamp)
        metadata.pure[F].pure[F]
      }

      def partitions(topic: Topic) = List.empty[PartitionInfo].pure[F]

      def flush = empty

      def clientMetrics = Seq.empty[ClientMetric[F]].pure[F]
    }
  }

  @deprecated("Use of(ProducerConfig)", since = "12.0.1")
  def of[F[_]: ToTry: Async](
    config: ProducerConfig,
    executorBlocking: ExecutionContext
  ): Resource[F, Producer[F]] = {
    of(config)
  }

  def of[F[_]: ToTry: Async](
    config: ProducerConfig
  ): Resource[F, Producer[F]] = {
    val producer = CreateProducerJ(config)
    fromProducerJ2(producer)
  }

  private sealed abstract class Main

  @deprecated("Use fromProducerJ2", since = "12.0.1")
  def fromProducerJ1[F[_]: Blocking: ToTry: Async](producer: F[ProducerJ[Bytes, Bytes]]): Resource[F, Producer[F]] = {
    fromProducerJ2(producer)
  }

  def fromProducerJ2[F[_]: ToTry: Async](producer: F[ProducerJ[Bytes, Bytes]]): Resource[F, Producer[F]] = {

    def blocking[A](f: => A) = Sync[F].blocking(f)

    def apply(producer: ProducerJ[Bytes, Bytes]) = {
      new Main with Producer[F] {

        def initTransactions = {
          blocking { producer.initTransactions() }
        }

        def beginTransaction = {
          Sync[F].delay { producer.beginTransaction() }
        }

        def commitTransaction = {
          blocking { producer.commitTransaction() }
        }

        def abortTransaction = {
          blocking { producer.abortTransaction() }
        }

        def send[K, V](record: ProducerRecord[K, V])(implicit toBytesK: ToBytes[F, K], toBytesV: ToBytes[F, V]) = {

          def executionException[A]: PartialFunction[Throwable, F[A]] = {
            case failure: ExecutionException => failure.getCause.raiseError[F, A]
          }

          def block(record: ProducerRecordJ[Bytes, Bytes]) = {

            def callbackOf(deferred: Deferred[F, Either[Throwable, RecordMetadataJ]]): Callback = {
              (metadata: RecordMetadataJ, exception: Exception) =>
                val result = if (exception != null) {
                  exception.asLeft[RecordMetadataJ]
                } else if (metadata != null) {
                  metadata.asRight[Throwable]
                } else {
                  SkafkaError("both metadata & exception are nulls").asLeft[RecordMetadataJ]
                }
                deferred
                  .complete(result)
                  .toTry
                  .get
                ()
            }

            val result = for {
              deferred <- Async[F].deferred[Either[Throwable, RecordMetadataJ]]
              callback  = callbackOf(deferred)
              _        <- blocking { producer.send(record, callback) }
            } yield {
              deferred
                .get
                .flatMap { _.liftTo[F] }
                .recoverWith(executionException)
            }
            result.recoverWith(executionException)
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

        def flush = {
          blocking { producer.flush() }
        }

        private val metricsProvider = ClientMetricsProvider[F](producer)

        def clientMetrics = metricsProvider.get
      }
    }

    for {
      producerJ <- producer.toResource
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

      def initTransactions = {
        for {
          d <- MeasureDuration[F].start
          r <- producer.initTransactions.attempt
          d <- d
          _ <- metrics.initTransactions(d)
          r <- r.liftTo[F]
        } yield r
      }

      def beginTransaction = {
        for {
          r <- producer.beginTransaction
          _ <- metrics.beginTransaction
        } yield r
      }

      def commitTransaction = {
        for {
          d <- MeasureDuration[F].start
          r <- producer.commitTransaction.attempt
          d <- d
          _ <- metrics.commitTransaction(d)
          r <- r.liftTo[F]
        } yield r
      }

      def abortTransaction = {
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

      def flush = {
        for {
          d <- MeasureDuration[F].start
          r <- producer.flush.attempt
          d <- d
          _ <- metrics.flush(d)
          r <- r.liftTo[F]
        } yield r
      }

      def clientMetrics = producer.clientMetrics
    }
  }

  private sealed abstract class MapK

  implicit class ProducerOps[F[_]](val self: Producer[F]) extends AnyVal {

    def withLogging(log: Log[F])(implicit F: MonadThrow[F], measureDuration: MeasureDuration[F]): Producer[F] = {
      ProducerLogging(self, log)
    }

    /** The sole purpose of this method is to support binary compatibility with an intermediate
     *  version (namely, 15.2.0) which had `withLogging` method using `MeasureDuration` from `smetrics`
     *  and `withLogging1` using `MeasureDuration` from `cats-helper`.
     *  This should not be used and should be removed in a reasonable amount of time.
     */
    @deprecated("Use `withLogging`", since = "16.0.2")
    def withLogging1(log: Log[F])(implicit F: MonadThrow[F], measureDuration: MeasureDuration[F]): Producer[F] = {
      withLogging(log)
    }

    /**
      * @param charsToTrim a number of chars from record's value to log when producing fails because of a too large record
      */
    def withLogging(
      log: Log[F],
      charsToTrim: Int
    )(implicit F: MonadThrow[F], measureDuration: MeasureDuration[F]): Producer[F] = {
      ProducerLogging(self, log, charsToTrim)
    }

    /**
     * The sole purpose of this method is to support binary compatibility with an intermediate
     * version (namely, 15.2.0) which had `withLogging` method using `MeasureDuration` from `smetrics`
     * and `withLogging1` using `MeasureDuration` from `cats-helper`.
     * This should not be used and should be removed in a reasonable amount of time.
     *
     * @param charsToTrim a number of chars from record's value to log when producing fails because of a too large record
     */
    @deprecated("Use `withLogging`", since = "16.0.2")
    def withLogging1(
      log: Log[F],
      charsToTrim: Int
    )(implicit F: MonadThrow[F], measureDuration: MeasureDuration[F]): Producer[F] = {
      withLogging(log, charsToTrim)
    }

    def withMetrics[E](
      metrics: ProducerMetrics[F]
    )(implicit F: MonadError[F, E], measureDuration: MeasureDuration[F]): Producer[F] = {
      Producer(self, metrics)
    }

    /** The sole purpose of this method is to support binary compatibility with an intermediate
      * version (namely, 15.2.0) which had `withMetrics` method using `MeasureDuration` from `smetrics`
      * and `withMetrics1` using `MeasureDuration` from `cats-helper`.
      * This should not be used and should be removed in a reasonable amount of time.
      */
    @deprecated("Use `withMetrics`", since = "16.0.2")
    def withMetrics1[E](
      metrics: ProducerMetrics[F]
    )(implicit F: MonadError[F, E], measureDuration: MeasureDuration[F]): Producer[F] = {
      withMetrics(metrics)
    }

    def mapK[G[_]: Functor](fg: F ~> G, gf: G ~> F)(implicit F: Monad[F]): Producer[G] = new MapK with Producer[G] {

      def initTransactions = fg(self.initTransactions)

      def beginTransaction = fg(self.beginTransaction)

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

      def clientMetrics = fg(self.clientMetrics.map(_.map(m => m.copy(value = fg(m.value)))))
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
