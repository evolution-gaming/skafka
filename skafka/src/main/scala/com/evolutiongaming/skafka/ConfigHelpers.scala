package com.evolutiongaming.skafka

import com.evolutiongaming.config.ConfigHelper.{ConfigOps, FromConf}
import com.typesafe.config.{Config, ConfigException, ConfigRenderOptions, ConfigValue}

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, SECONDS, TimeUnit}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object ConfigHelpers {

  implicit val ClassFromConf: FromConf[Class[_]] = FromConf[Class[_]] { (conf, path) =>
    val className = conf.getString(path)
    Try(Class.forName(className)) match {
      case Failure(_) => throw new ConfigException.BadValue(conf.origin(), path, s"Class '$className' doesn't exist")
      case Success(value) => value
    }
  }

  implicit val JaasOptionsFromConf: FromConf[Map[String, String]] = FromConf[Map[String, String]] {

    def asString(value: ConfigValue) = {
      value
        .render(ConfigRenderOptions.concise().setJson(false))
        .stripPrefix("\"") // sometimes pure config wrap value with quotes
        .stripSuffix("\"")
    }

    (conf, path) =>
      conf
        .getObject(path)
        .entrySet
        .asScala
        .map(entry => (entry.getKey, asString(entry.getValue)))
        .toMap
  }

  implicit val ConfigValueFromConfig: FromConf[ConfigValue] = (conf, path) => conf.getValue(path)

  implicit class ConfigHelpersOps(val config: Config) {
    def getMillis(path: String, pathWithUnit: => String): Option[FiniteDuration] =
      getDuration(path, MILLISECONDS, pathWithUnit)

    def getSeconds(path: String, pathWithUnit: => String): Option[FiniteDuration] =
      getDuration(path, SECONDS, pathWithUnit)

    private def getDuration(path: String, timeUnit: TimeUnit, pathWithUnit: => String) = {
      val value = Try(config.getOpt[FiniteDuration](path)) match {
        case Failure(_: ConfigException) => None
        case Failure(e)                  => throw e
        case Success(value)              => value
      }
      value orElse config.getOpt[Long](pathWithUnit).map { Duration(_, timeUnit) }
    }
  }

  implicit val KeystoreTypeFromConfig: FromConf[KeystoreType] = (conf, path) => {
    val value = conf.getString(path)
    KeystoreType
      .Values
      .find(_.name.equalsIgnoreCase(value))
      .getOrElse(
        throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse KeystoreType from '$value'")
      )
  }
}
