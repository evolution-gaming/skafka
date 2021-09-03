package com.evolutiongaming.skafka

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JaasConfigSpec extends AnyFunSuite with Matchers {

  test("parse plain SASL JAAS") {
    val config = ConfigFactory.parseURL(getClass.getResource("sasl-jaas-plain.conf"))
    JaasConfig(config.getValue("sasl-jaas-config"))
      .asString() shouldEqual
      "org.apache.kafka.common.security.scram.ScramLoginModule " +
      "required " +
      "username='user' " +
      "password='pass';"
  }

  test("parse structured SASL JAAS") {
    val config = ConfigFactory.parseURL(getClass.getResource("sasl-jaas-struct.conf"))
    JaasConfig(config.getValue("sasl-jaas-config"))
      .asString() shouldEqual "com.evolutiongaming.skafka.JaasConfigSpec " +
      "optional " +
      "key='value' " +
      "username='user' " +
      "password='pass';"
  }
}