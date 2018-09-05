package com.evolutiongaming.skafka

import com.evolutiongaming.nel.Nel
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class CommonConfigSpec extends FunSuite with Matchers {

  val custom = CommonConfig(
    bootstrapServers = Nel("host:port"),
    clientId = Some("clientId"),
    connectionsMaxIdle = 1.millis,
    receiveBufferBytes = 2,
    sendBufferBytes = 3,
    requestTimeout = 4.seconds,
    metadataMaxAge = 5.seconds,
    reconnectBackoffMax = 5.hours,
    reconnectBackoff = 6.millis,
    retryBackoff = 7.seconds,
    securityProtocol = SecurityProtocol.Ssl,
    metrics = MetricsConfig(
      sampleWindow = 9.hours,
      numSamples = 10,
      recordingLevel = "DEBUG",
      reporters = List("reporter")))


  test("apply from empty config") {
    val config = ConfigFactory.empty()
    CommonConfig(config) shouldEqual CommonConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("valid.conf"))
    CommonConfig(config) shouldEqual custom
  }

  test("apply from deprecated config") {
    val config = ConfigFactory.parseURL(getClass.getResource("deprecated.conf"))
    CommonConfig(config) shouldEqual custom
  }

  test("bindings") {
    val configs = CommonConfig(
      bootstrapServers = Nel("localhost:9092", "127.0.0.1:9092"),
      clientId = Some("clientId"))

    configs.bindings shouldEqual Map(
      "bootstrap.servers" -> "localhost:9092,127.0.0.1:9092",
      "client.id" -> "clientId",
      "connections.max.idle.ms" -> "540000",
      "receive.buffer.bytes" -> "32768",
      "send.buffer.bytes" -> "131072",
      "request.timeout.ms" -> "30000",
      "metadata.max.age.ms" -> "300000",
      "reconnect.backoff.max.ms" -> "1000",
      "retry.backoff.ms" -> "100",
      "security.protocol" -> "PLAINTEXT",
      "reconnect.backoff.ms" -> "50",
      "metrics.sample.window.ms" -> "30000",
      "metrics.num.samples" -> "2",
      "metrics.recording.level" -> "INFO",
      "metric.reporters" -> "")
  }
}