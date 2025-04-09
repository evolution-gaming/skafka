package com.evolutiongaming.skafka.producer

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.skafka.CommonConfig
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ProducerConfigSpec extends AnyFunSuite with Matchers {

  val custom = ProducerConfig(
    batchSize                        = 1,
    deliveryTimeout                  = 2.seconds,
    acks                             = Acks.All,
    linger                           = 3.millis,
    maxRequestSize                   = 4,
    maxBlock                         = 5.seconds,
    bufferMemory                     = 6,
    compressionType                  = CompressionType.Lz4,
    retries                          = 8,
    maxInFlightRequestsPerConnection = 7,
    interceptorClasses               = List("interceptorClasses"),
    idempotence                      = true,
    transactionTimeout               = 8.minute,
    transactionalId                  = Some("transactionalId")
  )

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
      common          = CommonConfig(bootstrapServers = Nel.of("localhost:9092", "127.0.0.1:9092"), clientId = Some("clientId")),
      transactionalId = Some("transactionId")
    )

    configs.bindings shouldEqual Map(
      "reconnect.backoff.max.ms"                 -> "1000",
      "retries"                                  -> "2147483647",
      "compression.type"                         -> "none",
      "buffer.memory"                            -> "33554432",
      "connections.max.idle.ms"                  -> "540000",
      "max.request.size"                         -> "1048576",
      "metrics.sample.window.ms"                 -> "30000",
      "security.protocol"                        -> "PLAINTEXT",
      "bootstrap.servers"                        -> "localhost:9092,127.0.0.1:9092",
      "request.timeout.ms"                       -> "30000",
      "max.block.ms"                             -> "60000",
      "client.id"                                -> "clientId",
      "metric.reporters"                         -> "",
      "transaction.timeout.ms"                   -> "60000",
      "transactional.id"                         -> "transactionId",
      "interceptor.classes"                      -> "",
      "delivery.timeout.ms"                      -> "120000",
      "acks"                                     -> "all",
      "metadata.max.age.ms"                      -> "300000",
      "enable.idempotence"                       -> "true",
      "metrics.num.samples"                      -> "2",
      "metrics.recording.level"                  -> "INFO",
      "max.in.flight.requests.per.connection"    -> "5",
      "retry.backoff.ms"                         -> "100",
      "receive.buffer.bytes"                     -> "32768",
      "reconnect.backoff.ms"                     -> "50",
      "linger.ms"                                -> "0",
      "batch.size"                               -> "16384",
      "send.buffer.bytes"                        -> "131072",
      "client.dns.lookup"                        -> "use_all_dns_ips",
      "metadata.max.idle.ms"                     -> "300000",
      "partitioner.adaptive.partitioning.enable" -> "true",
      "partitioner.availability.timeout.ms"      -> "0",
      "partitioner.ignore.keys"                  -> "false",
      "socket.connection.setup.timeout.max.ms"   -> "30000",
      "socket.connection.setup.timeout.ms"       -> "10000",
    )
  }
}
