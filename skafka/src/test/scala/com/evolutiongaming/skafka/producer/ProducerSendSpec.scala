package com.evolutiongaming.skafka.producer

import java.util.concurrent.CompletableFuture
import java.util.{Map => MapJ}

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, IO, Sync}
import cats.implicits._
import com.evolutiongaming.catshelper.{Blocking, FromFuture, FromTry, ToFuture, ToTry}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Bytes, Partition, TopicPartition}
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, ProducerRecord => ProducerRecordJ, RecordMetadata => RecordMetadataJ}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition => TopicPartitionJ}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.compat.java8.FutureConverters._
import scala.jdk.CollectionConverters._

class ProducerSendSpec extends AsyncFunSuite with Matchers {

  test("block & send") {
    import com.evolutiongaming.skafka.IOSuite._
    blockAndSend[IO].run()
  }

  private def blockAndSend[
    F[_]: ConcurrentEffect: ToTry: FromTry: ToFuture: FromFuture: ContextShift: Blocking
  ] = {

    val topic = "topic"
    val topicPartition = TopicPartition(topic = topic, partition = Partition.min)
    val metadata = RecordMetadata(topicPartition)
    val record = ProducerRecord(topic = topic, value = "val", key = "key")


    def producerOf(block: F[ProducerRecordJ[Bytes, Bytes] => F[RecordMetadataJ]]) = {
      val producer: ProducerJ[Bytes, Bytes] = new ProducerJ[Bytes, Bytes] {

        def initTransactions() = {}

        def beginTransaction() = {}

        def sendOffsetsToTransaction(
          offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ],
          groupMetadata: ConsumerGroupMetadata
        ) = {}

        def sendOffsetsToTransaction(
          offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ],
          consumerGroupId: String
        ) = {}

        def commitTransaction() = {}

        def flush() = {}

        def partitionsFor(topic: String) = Nil.asJava

        def metrics() = Map.empty[MetricName, Metric].asJava

        def close() = {}

        def close(timeout: java.time.Duration) = {}

        def send(record: ProducerRecordJ[Bytes, Bytes]) = {
          CompletableFuture.completedFuture(metadata.asJava)
        }

        def send(record: ProducerRecordJ[Bytes, Bytes], callback: Callback) = {
          val a = for {
            f <- block
          } yield {
            val a = for {
              a <- f(record).attempt
              _ <- a match {
                case Right(a)           => Sync[F].delay { callback.onCompletion(a, null) }
                case Left(a: Exception) => Sync[F].delay { callback.onCompletion(null, a) }
                case Left(a)            => a.raiseError[F, Unit]
              }
              a <- a.liftTo[F]
            } yield a
            ToFuture[F].apply { a }.toJava.toCompletableFuture
          }
          ToTry[F].apply(a).get
        }

        def abortTransaction() = {}
      }

      Producer.fromProducerJ(producer.pure[F])
    }

    def start[A](fa: F[A]) = {
      Sync[F].uncancelable {
        for {
          started <- Deferred[F, Unit]
          fiber   <- Concurrent[F].start {
            for {
              _ <- started.complete(())
              a <- fa
            } yield a
          }
          _       <- started.get
        } yield {
          fiber.join
        }
      }
    }

    val result = for {
      sendDef     <- Deferred[F, ProducerRecordJ[Bytes, Bytes] => F[RecordMetadataJ]].toResource
      metadataDef <- Deferred[F, RecordMetadataJ].toResource
      producer    <- producerOf(sendDef.get)
      blocked     <- start { producer.send(record) }.toResource
      send         = (_: ProducerRecordJ[Bytes, Bytes]) => metadataDef.get
      _           <- sendDef.complete(send).toResource
      blocked     <- blocked.toResource
      sent        <- start { blocked }.toResource
      _           <- metadataDef.complete(metadata.asJava).toResource
      result      <- sent.toResource
    } yield {
      result shouldEqual metadata
    }

    result.use { _.pure[F] }
  }
}
