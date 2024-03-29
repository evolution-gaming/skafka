package com.evolutiongaming.skafka

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JaasConfigSpec extends AnyFunSuite with Matchers {

  test("parse plain SASL JAAS") {
    val config = ConfigFactory.parseURL(getClass.getResource("sasl-jaas-plain.conf"))
    JaasConfig
      .fromConfig(config.getValue("sasl-jaas-config"))
      .asString() shouldEqual
      "org.apache.kafka.common.security.scram.ScramLoginModule " +
      "required " +
      "username='user' " +
      "password='pass';"
  }

  test("parse structured SASL JAAS") {
    val config = ConfigFactory.parseURL(getClass.getResource("sasl-jaas-struct.conf"))
    JaasConfig.fromConfig(config.getValue("sasl-jaas-config")).asString() should (
      startWith("com.evolutiongaming.skafka.JaasConfigSpec optional ") and
        include("key='value'") and
        include("username='123USER123'") and
        include("password='pass'") and
        endWith(";")
    )
  }
}
