package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.CommonConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class ConsumerConfigSpec extends FunSuite with Matchers {

  val custom = ConsumerConfig(
    groupId = Some("groupId"),
    maxPollRecords = 1,
    maxPollInterval = 2.millis,
    sessionTimeout = 3.seconds,
    heartbeatInterval = 4.minutes,
    enableAutoCommit = false,
    autoCommitInterval = 5.hours,
    partitionAssignmentStrategy = "partitionAssignmentStrategy",
    autoOffsetReset = "earliest",
    fetchMinBytes = 6,
    fetchMaxBytes = 7,
    fetchMaxWait = 8.millis,
    maxPartitionFetchBytes = 9,
    checkCrcs = false,
    interceptorClasses = List("interceptorClasses"),
    excludeInternalTopics = false,
    isolationLevel = "read_committed")

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    ConsumerConfig(config) shouldEqual ConsumerConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("valid.conf"))
    println(custom.bindings)
    ConsumerConfig(config) shouldEqual custom
  }

  test("apply from deprecated config") {
    val config = ConfigFactory.parseURL(getClass.getResource("deprecated.conf"))
    ConsumerConfig(config) shouldEqual custom
  }

  test("bindings") {
    val configs = ConsumerConfig(
      common = CommonConfig(
        bootstrapServers = Nel("localhost:9092", "127.0.0.1:9092"),
        clientId = Some("clientId")))

    configs.bindings shouldEqual Map(
      "exclude.internal.topics" -> "true",
      "reconnect.backoff.max.ms" -> "1000",
      "auto.offset.reset" -> "latest",
      "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor",
      "retries" -> "0",
      "heartbeat.interval.ms" -> "3000",
      "check.crcs" -> "true",
      "auto.commit.interval.ms" -> "5000",
      "connections.max.idle.ms" -> "540000",
      "fetch.max.wait.ms" -> "500",
      "fetch.min.bytes" -> "1",
      "metrics.sample.window.ms" -> "30000",
      "security.protocol" -> "PLAINTEXT",
      "group.id" -> "",
      "bootstrap.servers" -> "localhost:9092,127.0.0.1:9092",
      "enable.auto.commit" -> "true",
      "fetch.max.bytes" -> "52428800",
      "max.partition.fetch.bytes" -> "1048576",
      "request.timeout.ms" -> "30000",
      "max.poll.records" -> "500",
      "client.id" -> "clientId",
      "max.poll.interval.ms" -> "300000",
      "metric.reporters" -> "",
      "interceptor.classes" -> "",
      "metadata.max.age.ms" -> "300000",
      "metrics.num.samples" -> "2",
      "metrics.recording.level" -> "INFO",
      "retry.backoff.ms" -> "100",
      "session.timeout.ms" -> "10000",
      "receive.buffer.bytes" -> "32768",
      "reconnect.backoff.ms" -> "50",
      "isolation.level" -> "read_uncommitted",
      "send.buffer.bytes" -> "131072")
  }
}