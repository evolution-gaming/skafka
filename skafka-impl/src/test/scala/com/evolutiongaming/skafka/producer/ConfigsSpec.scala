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
      clientId = "clientId",
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
      clientId = "clientId",
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
}
