package com.evolutiongaming.skafka.consumer

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.skafka.JaasConfig.Plain
import com.evolutiongaming.skafka.{CommonConfig, KeystoreType, SaslSupportConfig, SslSupportConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

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
    isolationLevel              = IsolationLevel.ReadCommitted,
    saslSupport = SaslSupportConfig(
      kerberosServiceName             = Some("service_name"),
      kerberosKinitCmd                = "/bin/kinit",
      kerberosTicketRenewWindowFactor = 0.4,
      kerberosTicketRenewJitter       = 0.1,
      kerberosMinTimeBeforeRelogin    = 1000.millis,
      loginRefreshWindowFactor        = 0.2,
      loginRefreshWindowJitter        = 0.3,
      loginRefreshMinPeriod           = 20.seconds,
      loginRefreshBuffer              = 10.seconds,
      mechanism                       = "PLAIN",
      jaasConfig                      = Some(Plain("plain config")),
      clientCallbackHandlerClass      = Some(classOf[ConsumerConfigSpec]),
      loginCallbackHandlerClass       = Some(classOf[ConsumerConfigSpec]),
      loginClass                      = Some(classOf[ConsumerConfigSpec]),
    ),
    sslSupport = SslSupportConfig(
      keystoreType                    = Some(KeystoreType.JKS),
      keystoreKey                     = Some("---key1---"),
      keystoreCertificateChain        = Some("---chain---"),
      keystoreLocation                = Some("/tmp/keystore.jks"),
      keystorePassword                = Some("some password 1"),
      truststoreCertificates          = Some("---key2---"),
      truststoreType                  = Some(KeystoreType.JKS),
      truststoreLocation              = Some("/tmp/truststore.jks"),
      truststorePassword              = Some("some password 2"),
      endpointIdentificationAlgorithm = Some("algo")
    )
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
      common             = CommonConfig(bootstrapServers = Nel.of("localhost:9092", "127.0.0.1:9092"), clientId = Some("clientId")),
      autoCommitInterval = Some(5.seconds),
      saslSupport = new SaslSupportConfig(
        kerberosServiceName        = Some("service_name"),
        jaasConfig                 = Some(Plain("plain config")),
        clientCallbackHandlerClass = Some(classOf[ConsumerConfigSpec]),
        loginCallbackHandlerClass  = Some(classOf[ConsumerConfigSpec]),
        loginClass                 = Some(classOf[ConsumerConfigSpec])
      ),
      sslSupport = SslSupportConfig(
        keystoreType                    = Some(KeystoreType.JKS),
        keystoreKey                     = Some("---key1---"),
        keystoreCertificateChain        = Some("---chain---"),
        keystoreLocation                = Some("/tmp/keystore.jks"),
        keystorePassword                = Some("some password 1"),
        truststoreCertificates          = Some("---key2---"),
        truststoreType                  = Some(KeystoreType.JKS),
        truststoreLocation              = Some("/tmp/truststore.jks"),
        truststorePassword              = Some("some password 2"),
        endpointIdentificationAlgorithm = Some("algo")
      )
    )

    configs.bindings shouldEqual Map(
      "exclude.internal.topics"                  -> "true",
      "reconnect.backoff.max.ms"                 -> "1000",
      "auto.offset.reset"                        -> "latest",
      "partition.assignment.strategy"            -> "org.apache.kafka.clients.consumer.RangeAssignor",
      "heartbeat.interval.ms"                    -> "3000",
      "check.crcs"                               -> "true",
      "auto.commit.interval.ms"                  -> "5000",
      "default.api.timeout.ms"                   -> "60000",
      "connections.max.idle.ms"                  -> "540000",
      "fetch.max.wait.ms"                        -> "500",
      "fetch.min.bytes"                          -> "1",
      "metrics.sample.window.ms"                 -> "30000",
      "security.protocol"                        -> "PLAINTEXT",
      "bootstrap.servers"                        -> "localhost:9092,127.0.0.1:9092",
      "enable.auto.commit"                       -> "true",
      "fetch.max.bytes"                          -> "52428800",
      "max.partition.fetch.bytes"                -> "1048576",
      "request.timeout.ms"                       -> "30000",
      "max.poll.records"                         -> "500",
      "client.id"                                -> "clientId",
      "max.poll.interval.ms"                     -> "300000",
      "metric.reporters"                         -> "",
      "interceptor.classes"                      -> "",
      "metadata.max.age.ms"                      -> "300000",
      "metrics.num.samples"                      -> "2",
      "metrics.recording.level"                  -> "INFO",
      "retry.backoff.ms"                         -> "100",
      "session.timeout.ms"                       -> "10000",
      "receive.buffer.bytes"                     -> "32768",
      "reconnect.backoff.ms"                     -> "50",
      "isolation.level"                          -> "read_uncommitted",
      "send.buffer.bytes"                        -> "131072",
      "sasl.kerberos.service.name"               -> "service_name",
      "sasl.kerberos.kinit.cmd"                  -> "/usr/bin/kinit",
      "sasl.kerberos.min.time.before.relogin"    -> "60000",
      "sasl.kerberos.ticket.renew.window.factor" -> "0.8",
      "sasl.kerberos.ticket.renew.jitter"        -> "0.05",
      "sasl.login.refresh.window.factor"         -> "0.8",
      "sasl.login.refresh.window.jitter"         -> "0.05",
      "sasl.login.refresh.min.period.seconds"    -> "60",
      "sasl.login.refresh.buffer.seconds"        -> "300",
      "sasl.mechanism"                           -> "GSSAPI",
      "sasl.client.callback.handler.class"       -> "com.evolutiongaming.skafka.consumer.ConsumerConfigSpec",
      "sasl.login.callback.handler.class"        -> "com.evolutiongaming.skafka.consumer.ConsumerConfigSpec",
      "sasl.login.class"                         -> "com.evolutiongaming.skafka.consumer.ConsumerConfigSpec",
      "sasl.jaas.config"                         -> "plain config",
      "ssl.keystore.type"                        -> "JKS",
      "ssl.keystore.key"                         -> "---key1---",
      "ssl.keystore.certificate.chain"           -> "---chain---",
      "ssl.keystore.location"                    -> "/tmp/keystore.jks",
      "ssl.keystore.password"                    -> "some password 1",
      "ssl.truststore.certificates"              -> "---key2---",
      "ssl.truststore.type"                      -> "JKS",
      "ssl.truststore.location"                  -> "/tmp/truststore.jks",
      "ssl.truststore.password"                  -> "some password 2",
      "ssl.endpoint.identification.algorithm"    -> "algo",
      "client.dns.lookup"                        -> "use_all_dns_ips",
      "socket.connection.setup.timeout.max.ms"   -> "30000",
      "socket.connection.setup.timeout.ms"       -> "10000",
    )
  }
}
