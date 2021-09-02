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

sealed trait JaasConfig {
  def asString(): String
}

// org.apache.kafka.common.security.scram.ScramLoginModule required username='7N637LQ6JFF5M4L2' password='knGr/ANovu2+d7/S0TT05xhEL/CMt0C2xh7XejsMtaioGydm1WTwIs5A8z5Oi/jE';

object JaasConfig {

  type Option = (String, String)

  case class Plain(str: String) extends JaasConfig {
    override def asString(): String = str
  }

//    loginModuleClass controlFlag (optionName=optionValue)*;
  case class LoginContext(loginModuleClass: String, controlFlag: String, options: List[Option]) extends JaasConfig {
    override def asString(): String = s"$loginModuleClass $controlFlag ${optionsAsString()}"

    private def optionsAsString() =
      options
        .map(option => s"${option._1}='${option._2}'")
        .mkString("", " ", ";")
  }
}
