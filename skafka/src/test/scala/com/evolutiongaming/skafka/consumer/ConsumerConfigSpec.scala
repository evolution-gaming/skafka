package com.evolutiongaming.skafka.consumer

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.skafka.CommonConfig
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConsumerConfigSpec extends AnyFunSuite with Matchers {

  val custom = ConsumerConfig(
    groupId                     = Some("groupId"),
    maxPollRecords              = 1,
    maxPollInterval             = 2.millis,
    sessionTimeout              = 3.seconds,
    heartbeatInterval           = 4.minutes,
    autoCommit                  = false,
    autoCommitInterval          = Some(5.hours),
    partitionAssignmentStrategy = "partitionAssignmentStrategy",
    autoOffsetReset             = AutoOffsetReset.Earliest,
    defaultApiTimeout           = 6.seconds,
    fetchMinBytes               = 6,
    fetchMaxBytes               = 7,
    fetchMaxWait                = 8.millis,
    maxPartitionFetchBytes      = 9,
    checkCrcs                   = false,
    interceptorClasses          = List("interceptorClasses"),
    excludeInternalTopics       = false,
    isolationLevel              = IsolationLevel.ReadCommitted
  )

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    ConsumerConfig(config) shouldEqual ConsumerConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("valid.conf"))
    ConsumerConfig(config) shouldEqual custom
  }

  test("apply from deprecated config") {
    val config = ConfigFactory.parseURL(getClass.getResource("deprecated.conf"))
    ConsumerConfig(config) shouldEqual custom
  }

  test("bindings") {
    val configs = ConsumerConfig(
      common = CommonConfig(bootstrapServers = Nel.of("localhost:9092", "127.0.0.1:9092"), clientId = Some("clientId"))
    )

    configs.bindings shouldEqual Map(
      "exclude.internal.topics"       -> "true",
      "reconnect.backoff.max.ms"      -> "1000",
      "auto.offset.reset"             -> "latest",
      "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor",
      "heartbeat.interval.ms"         -> "3000",
      "check.crcs"                    -> "true",
      "auto.commit.interval.ms"       -> "5000",
      "default.api.timeout.ms"        -> "60000",
      "connections.max.idle.ms"       -> "540000",
      "fetch.max.wait.ms"             -> "500",
      "fetch.min.bytes"               -> "1",
      "metrics.sample.window.ms"      -> "30000",
      "security.protocol"             -> "PLAINTEXT",
      "bootstrap.servers"             -> "localhost:9092,127.0.0.1:9092",
      "enable.auto.commit"            -> "true",
      "fetch.max.bytes"               -> "52428800",
      "max.partition.fetch.bytes"     -> "1048576",
      "request.timeout.ms"            -> "30000",
      "max.poll.records"              -> "500",
      "client.id"                     -> "clientId",
      "max.poll.interval.ms"          -> "300000",
      "metric.reporters"              -> "",
      "interceptor.classes"           -> "",
      "metadata.max.age.ms"           -> "300000",
      "metrics.num.samples"           -> "2",
      "metrics.recording.level"       -> "INFO",
      "retry.backoff.ms"              -> "100",
      "session.timeout.ms"            -> "10000",
      "receive.buffer.bytes"          -> "32768",
      "reconnect.backoff.ms"          -> "50",
      "isolation.level"               -> "read_uncommitted",
      "send.buffer.bytes"             -> "131072"
    )
  }
}
