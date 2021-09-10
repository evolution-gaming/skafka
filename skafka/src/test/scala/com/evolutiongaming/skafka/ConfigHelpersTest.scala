package com.evolutiongaming.skafka

import cats.implicits._
import com.evolutiongaming.skafka.ConfigHelpers._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ConfigHelpersTest extends AnyWordSpec with Matchers {

  "config" should {

    "parsed to Class" in {
      val config = makeConfig(Map("param1" -> "com.evolutiongaming.skafka.ConfigHelpersTest").asJava)
      ClassFromConf(config, "param1") shouldEqual classOf[ConfigHelpersTest]
    }

    "parsed to JaasOptions" in {
      val config = makeConfig(Map("options" -> Map("param1" -> "value1").asJava).asJava)
      JaasOptionsFromConf(config, "options") shouldEqual Map("param1" -> "value1")
    }

    "parsed to ConfigValue" in {
      val config = makeConfig(Map[String, Object]("prop1" -> "value1").asJava)
      ConfigValueFromConfig(config, "prop1") shouldEqual ConfigValueFactory.fromAnyRef("value1")
    }
  }

  "config implicit" should {

    "parsed to milliseconds" in {
      val config = makeConfig(Map[String, Object]("prop1" -> "100ms").asJava)
      ConfigHelpersOps(config).getMillis("prop1", "prop1") shouldEqual 100.millis.some
    }

    "parsed to seconds" in {
      val config = makeConfig(Map[String, Object]("prop1" -> "12s").asJava)
      ConfigHelpersOps(config).getSeconds("prop1", "prop1") shouldEqual 12.seconds.some
    }
  }

  private def makeConfig(data: java.util.Map[String, _ <: Any]) = ConfigFactory.parseMap(data)
}
