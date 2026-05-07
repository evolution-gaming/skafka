package com.evolutiongaming.skafka

import com.evolutiongaming.config.ConfigHelper.ConfigOps
import com.evolutiongaming.skafka.ConfigHelpers.*
import com.typesafe.config.{ConfigException, ConfigObject, ConfigValue}

import scala.util.{Failure, Success, Try}

sealed trait JaasConfig {
  def asString(): String
}

object JaasConfig {

  private val emptyPath = "\"\""

  final case class Plain(entry: String) extends JaasConfig {
    override def asString(): String = entry
  }

  final case class Structured(loginModuleClass: Class[?], controlFlag: String, options: Map[String, String])
      extends JaasConfig {

    override def asString(): String = s"${loginModuleClass.getName} $controlFlag ${optionsAsString()}"

    private def optionsAsString(): String =
      options
        .map { case (key, value) => s"$key='$value'" }
        .mkString("", " ", ";")
  }

  case object Structured {
    def fromConfig(obj: ConfigObject): Option[Structured] = {
      val config = obj.toConfig
      for {
        loginModuleClass <- config.getOpt[Class[?]]("login-module-class")
        controlFlag      <- config.getOpt[String]("control-flag")
        options          <- config.getOpt[Map[String, String]]("options")
      } yield new Structured(loginModuleClass, controlFlag, options)
    }
  }

  def fromConfig(config: ConfigValue): JaasConfig = {

    val value = config.atPath(emptyPath)

    def getPlain: Option[Plain] = Try(value.getString(emptyPath)) match {
      case Failure(_: ConfigException.WrongType) => None
      case Failure(exception)                    => throw exception
      case Success(string)                       => Some(Plain(string))
    }

    def getStructured: Option[Structured] = Try(value.getObject(emptyPath)) match {
      case Failure(_: ConfigException.WrongType) => None
      case Failure(exception)                    => throw exception
      case Success(obj)                          => Structured.fromConfig(obj)
    }

    getPlain.orElse(getStructured) match {
      case Some(value) => value
      case None        =>
        throw new ConfigException.BadValue(
          value.origin(),
          emptyPath,
          "Unexpected format of JAAS. Should be string or object"
        )
    }
  }
}
