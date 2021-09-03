package com.evolutiongaming.skafka

import com.evolutiongaming.config.ConfigHelper.ConfigOps
import com.evolutiongaming.skafka.ConfigHelpers._
import com.typesafe.config.{Config, ConfigException, ConfigObject}

import scala.util.{Failure, Success, Try}

sealed trait JaasConfig {
  def asString(): String
}

object JaasConfig {

  case class Plain(entry: String) extends JaasConfig {
    override def asString(): String = entry
  }

  case class Structured(loginModuleClass: Class[_], controlFlag: String, options: List[Pair]) extends JaasConfig {

    override def asString(): String = s"${loginModuleClass.getName} $controlFlag ${pairAsString()}"

    private def pairAsString() =
      options
        .map(option => s"${option._1}='${option._2}'")
        .mkString("", " ", ";")
  }

  case object Structured {
    def make(obj: ConfigObject): Option[Structured] = {
      val config = obj.toConfig
      for {
        loginModuleClass <- config.getOpt[Class[_]]("loginModuleClass")
        controlFlag      <- config.getOpt[String]("controlFlag")
        options          <- config.getOpt[List[Pair]]("options")
      } yield new Structured(loginModuleClass, controlFlag, options)
    }
  }

  def apply(config: Config): JaasConfig = {

    def getPlain = Try(config.getString("")) match {
      case Failure(_: ConfigException.WrongType) => None
      case Failure(exception)                    => throw exception
      case Success(string)                       => Some(Plain(string))
    }

    def getStructured = Try(config.getObject("")) match {
      case Failure(_: ConfigException.WrongType) => None
      case Failure(exception)                    => throw exception
      case Success(obj)                          => Structured.make(obj)
    }

    getPlain.orElse(getStructured) match {
      case Some(value) => value
      case None =>
        throw new ConfigException.BadValue(config.origin(), ".", "Unexpected format. Should be string or object")
    }
  }
}
