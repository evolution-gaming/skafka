package com.evolutiongaming.skafka.producer

import java.util.concurrent.CompletableFuture
import java.util.{Map => MapJ}

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ContextShift, IO, Sync}
import cats.implicits._
import com.evolutiongaming.catshelper.{FromFuture, FromTry, ToFuture, ToTry}
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.{Blocking, Bytes, TopicPartition}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, ProducerRecord => ProducerRecordJ, RecordMetadata => RecordMetadataJ}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition => TopicPartitionJ}
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext

class ProducerSendSpec extends AsyncFunSuite with Matchers {

  test("block & send") {
    import com.evolutiongaming.skafka.IOSuite._
    blockAndSend[IO](executor).run()
  }

  private def blockAndSend[F[_] : Concurrent : ToTry : FromTry : ToFuture : FromFuture : ContextShift](
    executor: ExecutionContext
  ) = {

    val topic = "topic"
    val topicPartition = TopicPartition(topic = topic, partition = 0)
    val metadata = RecordMetadata(topicPartition)
    val record = ProducerRecord(topic = topic, value = "val", key = "key")


    def producerOf(block: F[ProducerRecordJ[Bytes, Bytes] => F[RecordMetadataJ]]) = {
      val producer = new ProducerJ[Bytes, Bytes] {

        def sendOffsetsToTransaction(offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ], consumerGroupId: String) = {}

        def initTransactions() = {}

        def beginTransaction() = {}

        def flush() = {}

        def commitTransaction() = {}

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
              a <- a.raiseOrPure[F]
            } yield a
            ToFuture[F].apply { a }.toJava.toCompletableFuture
          }
          ToTry[F].apply(a).get
        }

        def abortTransaction() = {}
      }

      Producer[F](producer, Blocking(executor))
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

    for {
      sendDef     <- Deferred[F, ProducerRecordJ[Bytes, Bytes] => F[RecordMetadataJ]]
      metadataDef <- Deferred[F, RecordMetadataJ]
      producer     = producerOf(sendDef.get)
      blocked     <- start { producer.send(record) }
      send         = (_: ProducerRecordJ[Bytes, Bytes]) => metadataDef.get
      _           <- sendDef.complete(send)
      blocked     <- blocked
      sent        <- start { blocked }
      _           <- metadataDef.complete(metadata.asJava)
      result      <- sent
    } yield {
      result shouldEqual metadata
    }
  }
}
