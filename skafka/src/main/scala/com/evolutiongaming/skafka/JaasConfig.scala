package com.evolutiongaming.skafka

import com.evolutiongaming.config.ConfigHelper.ConfigOps
import com.evolutiongaming.skafka.ConfigHelpers._
import com.typesafe.config.{ConfigException, ConfigObject, ConfigValue}

import scala.util.{Failure, Success, Try}

sealed trait JaasConfig {
  def asString(): String
}

object JaasConfig {

  private val emptyPath = "\"\""

  case class Plain(entry: String) extends JaasConfig {
    override def asString(): String = entry
  }

  case class Structured(loginModuleClass: Class[_], controlFlag: String, options: List[Pair]) extends JaasConfig {

    override def asString(): String = s"${loginModuleClass.getName} $controlFlag ${pairAsString()}"

    private def pairAsString() =
      options
        .map { case (key, value) => s"$key='$value'" }
        .mkString("", " ", ";")
  }

  case object Structured {
    def make(obj: ConfigObject): Option[Structured] = {
      val config = obj.toConfig
      for {
        loginModuleClass <- config.getOpt[Class[_]]("login-module-class")
        controlFlag      <- config.getOpt[String]("control-flag")
        options          <- config.getOpt[List[Pair]]("options")
      } yield new Structured(loginModuleClass, controlFlag, options)
    }
  }

  def apply(config: ConfigValue): JaasConfig = {

    val value = config.atPath(emptyPath)

    def getPlain = Try(value.getString(emptyPath)) match {
      case Failure(_: ConfigException.WrongType) => None
      case Failure(exception)                    => throw exception
      case Success(string)                       => Some(Plain(string))
    }

    def getStructured = Try(value.getObject(emptyPath)) match {
      case Failure(_: ConfigException.WrongType) => None
      case Failure(exception)                    => throw exception
      case Success(obj)                          => Structured.make(obj)
    }

    getPlain.orElse(getStructured) match {
      case Some(value) => value
      case None =>
        throw new ConfigException.BadValue(
          value.origin(),
          emptyPath,
          "Unexpected format of JAAS. Should be string or object"
        )
    }
  }
}
