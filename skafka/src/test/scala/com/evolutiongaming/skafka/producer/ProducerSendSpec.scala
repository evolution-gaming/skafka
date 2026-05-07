package com.evolutiongaming.skafka.producer

import java.util.concurrent.{CompletableFuture, Future as FutureJ}
import java.util.Map as MapJ
import cats.effect.{Async, Concurrent, Deferred, IO, Resource, Sync}
import cats.implicits.*
import cats.effect.implicits.*
import com.evolutiongaming.catshelper.{FromTry, ToFuture, ToTry}
import com.evolutiongaming.skafka.producer.ProducerConverters.*
import com.evolutiongaming.skafka.{Bytes, Partition, TopicPartition}
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata as OffsetAndMetadataJ}
import org.apache.kafka.clients.producer.{
  Callback,
  Producer as ProducerJ,
  ProducerRecord as ProducerRecordJ,
  RecordMetadata as RecordMetadataJ
}
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, Uuid, TopicPartition as TopicPartitionJ}
import org.scalatest.Assertion
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import java.util
import scala.compat.java8.FutureConverters.*
import scala.jdk.CollectionConverters.*

class ProducerSendSpec extends AsyncFunSuite with Matchers {

  test("block & send") {
    import com.evolutiongaming.skafka.IOSuite.*
    blockAndSend[IO].run()
  }

  private def blockAndSend[
    F[_]: ToTry: FromTry: ToFuture: Async
  ]: F[Assertion] = {

    val topic          = "topic"
    val topicPartition = TopicPartition(topic = topic, partition = Partition.min)
    val metadata       = RecordMetadata(topicPartition)
    val record         = ProducerRecord(topic = topic, value = "val", key = "key")

    def producerOf(block: F[ProducerRecordJ[Bytes, Bytes] => F[RecordMetadataJ]]): Resource[F, Producer[F]] = {
      val producer: ProducerJ[Bytes, Bytes] = new ProducerJ[Bytes, Bytes] {

        def initTransactions(): Unit = {}

        def beginTransaction(): Unit = {}

        def sendOffsetsToTransaction(
          offsets: MapJ[TopicPartitionJ, OffsetAndMetadataJ],
          groupMetadata: ConsumerGroupMetadata
        ): Unit = {}

        def commitTransaction(): Unit = {}

        def flush(): Unit = {}

        def partitionsFor(topic: String): util.List[PartitionInfo] = Nil.asJava

        def metrics(): MapJ[MetricName, Metric] = Map.empty.asJava

        def clientInstanceId(timeout: java.time.Duration): Uuid = Uuid.ONE_UUID

        def close(): Unit = {}

        def close(timeout: java.time.Duration): Unit = {}

        def registerMetricForSubscription(metric: KafkaMetric): Unit = {}

        def unregisterMetricFromSubscription(metric: KafkaMetric): Unit = {}

        def send(record: ProducerRecordJ[Bytes, Bytes]): FutureJ[RecordMetadataJ] = {
          CompletableFuture.completedFuture(metadata.asJava)
        }

        def send(record: ProducerRecordJ[Bytes, Bytes], callback: Callback): FutureJ[RecordMetadataJ] = {
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

        def abortTransaction(): Unit = {}
      }

      Producer.fromProducerJ2(producer.pure[F])
    }

    def start[A](fa: F[A]): F[F[A]] = {
      Sync[F].uncancelable { _ =>
        for {
          started <- Deferred[F, Unit]
          fiber   <- Concurrent[F].start {
            for {
              _ <- started.complete(())
              a <- fa
            } yield a
          }
          _ <- started.get
        } yield {
          fiber.joinWithNever
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
