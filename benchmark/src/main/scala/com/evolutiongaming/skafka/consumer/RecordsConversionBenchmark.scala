package com.evolutiongaming.skafka.consumer

import cats.effect.IO
import com.evolutiongaming.skafka.consumer.ConsumerConverters.*
import org.apache.kafka.clients.consumer.{
  ConsumerRecord as ConsumerRecordJ,
  ConsumerRecords as ConsumerRecordsJ,
  OffsetAndMetadata as OffsetAndMetadataJ
}
import org.apache.kafka.common.TopicPartition as TopicPartitionJ
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.Optional
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

/** Cost of converting one Java poll result into skafka [[ConsumerRecords]], which `Consumer.poll` does on every poll.
  *
  * The records have `String` keys, `Unit` values and one header of 200 bytes, spread over 13 partitions of one topic.
  * `recordsPerPoll` covers a single record, a typical poll and a large catch-up poll. The conversion is run
  * synchronously in the benchmark thread, so no thread handoff is measured.
  *
  * To run: {{{sbt "benchmark/Jmh/run com.evolutiongaming.skafka.consumer.RecordsConversionBenchmark"}}}
  */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
class RecordsConversionBenchmark {

  @Param(Array("1", "25", "500"))
  var recordsPerPoll: Int = 0

  // JMH drives state through mutable fields and lifecycle hooks, so `var` is required here.
  private var recordsJ: ConsumerRecordsJ[String, Unit] = ConsumerRecordsJ.empty[String, Unit]()

  @Setup(Level.Trial)
  def setup(): Unit = {
    recordsJ = RecordsConversionBenchmark.recordsJ(recordsPerPoll)
  }

  @Benchmark
  def asScala(hole: Blackhole): Unit = {
    hole.consume(recordsJ.asScala[IO].syncStep(Int.MaxValue).unsafeRunSync())
  }
}

object RecordsConversionBenchmark {

  private val Topic      = "topic"
  private val Partitions = 13
  private val Header     = Array.fill[Byte](200)(1)

  private def recordsJ(records: Int): ConsumerRecordsJ[String, Unit] = {
    val byPartition = (0 until records)
      .map { index => recordJ(partition = index % Partitions, offset = index.toLong) }
      .groupBy { _.partition }
      .map { case (partition, records) => (new TopicPartitionJ(Topic, partition), records.toList.asJava) }
    new ConsumerRecordsJ[String, Unit](byPartition.asJava, Map.empty[TopicPartitionJ, OffsetAndMetadataJ].asJava)
  }

  private def recordJ(partition: Int, offset: Long): ConsumerRecordJ[String, Unit] = {
    val headers = new RecordHeaders()
    headers.add("header", Header)
    new ConsumerRecordJ[String, Unit](
      Topic,
      partition,
      offset,
      System.currentTimeMillis(),
      TimestampType.CREATE_TIME,
      36,
      800,
      s"key-$offset",
      (),
      headers,
      Optional.empty[Integer](),
    )
  }
}
