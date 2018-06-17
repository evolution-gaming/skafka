package com.evolutiongaming.skafka.producer

import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.CommonConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class ProducerConfigSpec extends FunSuite with Matchers {

  val custom = ProducerConfig(
    batchSize = 1,
    acks = Acks.All,
    linger = 3.millis,
    maxRequestSize = 4,
    maxBlock = 5.seconds,
    bufferMemory = 6,
    compressionType = CompressionType.Lz4,
    retries = 8,
    maxInFlightRequestsPerConnection = 7,
    partitionerClass = "partitionerClass",
    interceptorClasses = List("interceptorClasses"),
    enableIdempotence = true,
    transactionTimeout = 8.minute,
    transactionalId = Some("transactionalId"))

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    ProducerConfig(config) shouldEqual ProducerConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("valid.conf"))
    ProducerConfig(config) shouldEqual custom
  }

  test("apply from deprecated config") {
    val config = ConfigFactory.parseURL(getClass.getResource("deprecated.conf"))
    ProducerConfig(config) shouldEqual custom
  }

  test("bindings") {
    val configs = ProducerConfig(
      common = CommonConfig(
        bootstrapServers = Nel("localhost:9092", "127.0.0.1:9092"),
        clientId = Some("clientId")))

    configs.bindings shouldEqual Map(
      "reconnect.backoff.max.ms" -> "1000",
      "retries" -> "0",
      "compression.type" -> "none",
      "buffer.memory" -> "33554432",
      "connections.max.idle.ms" -> "540000",
      "partitioner.class" -> "org.apache.kafka.clients.producer.internals.DefaultPartitioner",
      "max.request.size" -> "1048576",
      "metrics.sample.window.ms" -> "30000",
      "security.protocol" -> "PLAINTEXT",
      "bootstrap.servers" -> "localhost:9092,127.0.0.1:9092",
      "request.timeout.ms" -> "30000",
      "max.block.ms" -> "60000",
      "client.id" -> "clientId",
      "metric.reporters" -> "",
      "transaction.timeout.ms" -> "60000",
      "interceptor.classes" -> "",
      "acks" -> "1",
      "metadata.max.age.ms" -> "300000",
      "enable.idempotence" -> "false",
      "metrics.num.samples" -> "2",
      "metrics.recording.level" -> "INFO",
      "max.in.flight.requests.per.connection" -> "5",
      "retry.backoff.ms" -> "100",
      "receive.buffer.bytes" -> "32768",
      "reconnect.backoff.ms" -> "50",
      "linger.ms" -> "0",
      "batch.size" -> "16384",
      "send.buffer.bytes" -> "131072")
  }
}
