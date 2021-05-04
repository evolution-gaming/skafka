package com.evolutiongaming.skafka

sealed trait SecurityProtocol extends Product {
  def name: String
}

object SecurityProtocol {
  val Values: Set[SecurityProtocol] = Set(Plaintext, Ssl, SaslPlaintext, SaslSsl)

  case object Plaintext extends SecurityProtocol { def name = "PLAINTEXT" }
  case object Ssl extends SecurityProtocol { def name = "SSL" }
  case object SaslPlaintext extends SecurityProtocol { def name = "SASL_PLAINTEXT" }
  case object SaslSsl extends SecurityProtocol { def name = "SASL_SSL" }
}
