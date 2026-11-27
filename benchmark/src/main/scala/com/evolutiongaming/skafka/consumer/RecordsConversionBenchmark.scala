package com.evolutiongaming.skafka.consumer

import cats.effect.IO
import cats.effect.unsafe.IORuntime
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

import java.util.{ArrayList as ArrayListJ, HashMap as HashMapJ, List as ListJ, Optional}
import java.util.concurrent.TimeUnit
import scala.util.Try

/** Cost of converting one Java poll result into skafka [[ConsumerRecords]], which `Consumer.poll` does on every poll.
  *
  * The records have `String` keys, `Unit` values and one header of 200 bytes, spread over 13 partitions of one topic,
  * in the Java collections `KafkaConsumer` uses. `recordsPerPoll` covers a single record, a typical poll and two large
  * catch-up polls. `ioRuntime` runs the conversion in `IO` on the runtime, as a consumer does, and `tryEffect` in a
  * strict effect.
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

  @Param(Array("1", "25", "500", "3000"))
  var recordsPerPoll: Int = 0

  private implicit val runtime: IORuntime = IORuntime.global

  // JMH drives state through mutable fields and lifecycle hooks, so `var` is required here.
  private var recordsJ: ConsumerRecordsJ[String, Unit] = ConsumerRecordsJ.empty[String, Unit]()

  @Setup(Level.Trial)
  def setup(): Unit = {
    recordsJ = RecordsConversionBenchmark.recordsJ(recordsPerPoll)
  }

  /** `IO` run by the runtime on its compute pool, as in a consumer. One invocation runs `IoBatch` conversions in one
    * `IO`, so the handoff between the benchmark thread and the pool is spread over them.
    */
  @Benchmark
  @OperationsPerInvocation(RecordsConversionBenchmark.IoBatch)
  def ioRuntime(hole: Blackhole): Unit = {
    recordsJ
      .asScala[IO]
      .map { records => hole.consume(records) }
      .replicateA_(RecordsConversionBenchmark.IoBatch)
      .unsafeRunSync()
  }

  @Benchmark
  def tryEffect(hole: Blackhole): Unit = {
    hole.consume(recordsJ.asScala[Try])
  }
}

object RecordsConversionBenchmark {

  final val IoBatch = 100

  private val Topic      = "topic"
  private val Partitions = 13
  private val Header     = Array.fill[Byte](200)(1)

  /** Built like `KafkaConsumer` builds a poll result: a `java.util.HashMap` of `java.util.ArrayList`s. Scala
    * collections behind `asJava` would allocate on every lookup and iteration, which real polls do not.
    */
  private def recordsJ(records: Int): ConsumerRecordsJ[String, Unit] = {
    val byPartition = new HashMapJ[TopicPartitionJ, ListJ[ConsumerRecordJ[String, Unit]]]()
    (0 until records).foreach { index =>
      val partition = index % Partitions
      byPartition
        .computeIfAbsent(new TopicPartitionJ(Topic, partition), (_: TopicPartitionJ) => new ArrayListJ())
        .add(recordJ(partition = partition, offset = index.toLong))
    }
    new ConsumerRecordsJ[String, Unit](byPartition, new HashMapJ[TopicPartitionJ, OffsetAndMetadataJ]())
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
