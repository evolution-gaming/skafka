package com.evolutiongaming.skafka

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.{Config, ConfigException}
import org.apache.kafka.clients.{CommonClientConfigs => C}

import scala.concurrent.duration.{FiniteDuration, _}

/**
  * @param bootstrapServers should be in the form of "host1:port1","host2:port2,..."
  * @param clientId         An id string to pass to the server when making requests
  */
final case class CommonConfig(
  bootstrapServers: Nel[String]                   = Nel.of("localhost:9092"),
  clientId: Option[ClientId]                      = None,
  connectionsMaxIdle: FiniteDuration              = 9.minutes,
  receiveBufferBytes: Int                         = 32768,
  sendBufferBytes: Int                            = 131072,
  requestTimeout: FiniteDuration                  = 30.seconds,
  metadataMaxAge: FiniteDuration                  = 5.minutes,
  reconnectBackoffMax: FiniteDuration             = 1.second,
  reconnectBackoff: FiniteDuration                = 50.millis,
  retryBackoff: FiniteDuration                    = 100.millis,
  securityProtocol: SecurityProtocol              = SecurityProtocol.Plaintext,
  metrics: MetricsConfig                          = MetricsConfig.Default,
  clientDnsLookup: ClientDnsLookup                = ClientDnsLookup.UseAllDnsIps,
  socketConnectionSetupTimeoutMax: FiniteDuration = 30.seconds,
  socketConnectionSetupTimeout: FiniteDuration    = 10.seconds,
) {

  def bindings: Map[String, String] = {
    val bindings = Map[String, String](
      (C.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.toList mkString ","),
      (C.CLIENT_ID_CONFIG, clientId getOrElse ""),
      (C.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdle.toMillis.toString),
      (C.RECEIVE_BUFFER_CONFIG, receiveBufferBytes.toString),
      (C.SEND_BUFFER_CONFIG, sendBufferBytes.toString),
      (C.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toMillis.toString),
      (C.METADATA_MAX_AGE_CONFIG, metadataMaxAge.toMillis.toString),
      (C.RECONNECT_BACKOFF_MAX_MS_CONFIG, reconnectBackoffMax.toMillis.toString),
      (C.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoff.toMillis.toString),
      (C.RETRY_BACKOFF_MS_CONFIG, retryBackoff.toMillis.toString),
      (C.SECURITY_PROTOCOL_CONFIG, securityProtocol.name),
      (C.CLIENT_DNS_LOOKUP_CONFIG, clientDnsLookup.name),
      (C.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, socketConnectionSetupTimeoutMax.toMillis.toString),
      (C.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, socketConnectionSetupTimeout.toMillis.toString),
    )

    bindings ++ metrics.bindings
  }
}

object CommonConfig {

  val Default: CommonConfig = CommonConfig()

  private implicit val SecurityProtocolFromConf = FromConf[SecurityProtocol] { (conf, path) =>
    val str   = conf.getString(path)
    val value = SecurityProtocol.Values.find { _.name equalsIgnoreCase str }
    value getOrElse {
      throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse SecurityProtocol from $str")
    }
  }

  private implicit val ClientDnsLookupFromConf = FromConf[ClientDnsLookup] { (conf, path) =>
    val str   = conf.getString(path)
    val value = ClientDnsLookup.Values.find { _.name.equalsIgnoreCase(str) }
    value.getOrElse(throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse ClientDnsLookup from $str"))
  }

  def apply(config: Config): CommonConfig = {
    apply(config, Default)
  }

  def apply(config: Config, default: => CommonConfig): CommonConfig = {

    def get[T: FromConf](path: String, paths: String*) = {
      config.getOpt[T](path, paths: _*)
    }

    def getDuration(path: String, pathMs: => String) = {
      val value =
        try get[FiniteDuration](path)
        catch { case _: ConfigException => None }
      value orElse get[Long](pathMs).map { _.millis }
    }

    CommonConfig(
      bootstrapServers = get[Nel[String]]("bootstrap-servers", "bootstrap.servers") getOrElse default.bootstrapServers,
      clientId         = get[String]("client-id", "client.id") orElse default.clientId,
      connectionsMaxIdle =
        getDuration("connections-max-idle", "connections.max.idle.ms") getOrElse default.connectionsMaxIdle,
      receiveBufferBytes =
        get[Int]("receive-buffer-bytes", "receive.buffer.bytes") getOrElse default.receiveBufferBytes,
      sendBufferBytes = get[Int]("send-buffer-bytes", "send.buffer.bytes") getOrElse default.sendBufferBytes,
      requestTimeout  = getDuration("request-timeout", "request.timeout.ms") getOrElse default.requestTimeout,
      metadataMaxAge  = getDuration("metadata-max-age", "metadata.max.age.ms") getOrElse default.metadataMaxAge,
      reconnectBackoffMax =
        getDuration("reconnect-backoff-max", "reconnect.backoff.max.ms") getOrElse default.reconnectBackoffMax,
      reconnectBackoff = getDuration("reconnect-backoff", "reconnect.backoff.ms") getOrElse default.reconnectBackoff,
      retryBackoff     = getDuration("retry-backoff", "retry.backoff.ms") getOrElse default.retryBackoff,
      securityProtocol =
        get[SecurityProtocol]("security-protocol", "security.protocol") getOrElse default.securityProtocol,
      metrics = MetricsConfig(config),
      clientDnsLookup =
        get[ClientDnsLookup]("client-dns-lookup", "client.dns.lookup") getOrElse default.clientDnsLookup,
      socketConnectionSetupTimeoutMax =
        getDuration("socket-connection-setup-timeout-max", "socket.connection.setup.timeout.max.ms").getOrElse(
          default.socketConnectionSetupTimeoutMax
        ),
      socketConnectionSetupTimeout =
        getDuration("socket-connection-setup-timeout", "socket.connection.setup.timeout.ms").getOrElse(
          default.socketConnectionSetupTimeout
        ),
    )
  }

  implicit def nelFromConf[T](implicit fromConf: FromConf[List[T]]): FromConf[Nel[T]] = {
    FromConf {
      case (config, path) =>
        val list = fromConf(config, path)
        Nel.fromListUnsafe(list)
    }
  }
}
