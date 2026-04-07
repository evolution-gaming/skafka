package com.evolutiongaming.skafka

import cats.implicits._
import com.evolutiongaming.config.ConfigHelper.ConfigOps
import com.evolutiongaming.skafka.ConfigHelpers.KeystoreTypeFromConfig
import com.typesafe.config.Config
import org.apache.kafka.common.config.SslConfigs

final case class SslSupportConfig(
  keystoreType: Option[KeystoreType]              = None,
  keystoreKey: Option[String]                     = None,
  keystoreCertificateChain: Option[String]        = None,
  keystoreLocation: Option[String]                = None,
  keystorePassword: Option[String]                = None,
  truststoreCertificates: Option[String]          = None,
  truststoreType: Option[KeystoreType]            = None,
  truststoreLocation: Option[String]              = None,
  truststorePassword: Option[String]              = None,
  endpointIdentificationAlgorithm: Option[String] = None,
) {
  def bindings: Map[String, String] = Map[String, Option[String]](
    (SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keystoreType.map(_.name)),
    (SslConfigs.SSL_KEYSTORE_KEY_CONFIG, keystoreKey),
    (SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, keystoreCertificateChain),
    (SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation),
    (SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword),
    (SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, truststoreCertificates),
    (SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststoreType.map(_.name)),
    (SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation),
    (SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword),
    (SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, endpointIdentificationAlgorithm),
  ).flattenOption
}

object SslSupportConfig {
  val Default: SslSupportConfig = SslSupportConfig()

  def apply(config: Config): SslSupportConfig =
    new SslSupportConfig(
      keystoreType             = config.getOpt[KeystoreType]("ssl-keystore-type", "ssl.keystore.type"),
      keystoreKey              = config.getOpt[String]("ssl-keystore-key", "ssl.keystore.key"),
      keystoreCertificateChain =
        config.getOpt[String]("ssl-keystore-certificate-chain", "ssl.keystore.certificate.chain"),
      keystoreLocation       = config.getOpt[String]("ssl-keystore-location", "ssl.keystore.location"),
      keystorePassword       = config.getOpt[String]("ssl-keystore-password", "ssl.keystore.password"),
      truststoreCertificates = config.getOpt[String]("ssl-truststore-certificates", "ssl.truststore.certificates"),
      truststoreType         = config.getOpt[KeystoreType]("ssl-truststore-type", "ssl.truststore.type"),
      truststoreLocation     = config.getOpt[String]("ssl-truststore-location", "ssl.truststore.location"),
      truststorePassword     = config.getOpt[String]("ssl-truststore-password", "ssl.truststore.password"),
      endpointIdentificationAlgorithm =
        config.getOpt[String]("ssl-endpoint-identification-algorithm", "ssl.endpoint.identification.algorithm"),
    )
}
