package com.evolutiongaming.skafka.producer

import com.evolutiongaming.nel.Nel
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class ConfigsSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    Configs(config) shouldEqual Configs.Default
  }

  test("apply from config") {
    getClass.getResource("desired.conf")
    val config = ConfigFactory.parseURL(getClass.getResource("desired.conf"))
    Configs(config) shouldEqual Configs(
      bootstrapServers = Nel("host:port"),
      clientId = Some("clientId"),
      acks = "all",
      bufferMemory = 1,
      compressionType = "lz4",
      batchSize = 2,
      connectionsMaxIdle = 3.millis,
      linger = 4.millis,
      maxBlock = 5.seconds,
      maxRequestSize = 6,
      receiveBufferBytes = 7,
      requestTimeout = 8.minutes,
      sendBufferBytes = 9,
      enableIdempotence = true,
      maxInFlightRequestsPerConnection = 11,
      metadataMaxAge = 12.hours,
      reconnectBackoffMax = 13.millis,
      reconnectBackoff = 14.seconds,
      retryBackoff = 15.minutes)
  }

  test("apply from deprecated config") {
    val config = ConfigFactory.parseURL(getClass.getResource("undesired.conf"))
    Configs(config) shouldEqual Configs(
      bootstrapServers = Nel("host:port"),
      clientId = Some("clientId"),
      acks = "all",
      bufferMemory = 1,
      compressionType = "lz4",
      batchSize = 2,
      connectionsMaxIdle = 3.millis,
      linger = 4.millis,
      maxBlock = 5.millis,
      maxRequestSize = 6,
      receiveBufferBytes = 7,
      requestTimeout = 8.millis,
      sendBufferBytes = 9,
      enableIdempotence = true,
      maxInFlightRequestsPerConnection = 11,
      metadataMaxAge = 12.millis,
      reconnectBackoffMax = 13.millis,
      reconnectBackoff = 14.millis,
      retryBackoff = 15.millis)
  }

  test("bindings") {
    val configs = Configs(
      bootstrapServers = Nel("localhost:9092", "127.0.0.1:9092"),
      clientId = Some("clientId"))

    configs.bindings shouldEqual Map(
      "reconnect.backoff.max.ms" -> "1000",
      "compression.type" -> "none",
      "buffer.memory" -> "33554432",
      "connections.max.idle.ms" -> "540000",
      "max.request.size" -> "1048576",
      "bootstrap.servers" -> "localhost:9092,127.0.0.1:9092",
      "request.timeout.ms" -> "30000",
      "max.block.ms" -> "60000",
      "client.id" -> "clientId",
      "acks" -> "1",
      "metadata.max.age.ms" -> "300000",
      "enable.idempotence" -> "false",
      "max.in.flight.requests.per.connection" -> "5",
      "retry.backoff.ms" -> "100",
      "receive.buffer.bytes" -> "32768",
      "reconnect.backoff.ms" -> "50",
      "linger.ms" -> "0",
      "batch.size" -> "16384",
      "send.buffer.bytes" -> "131072")
  }
}
