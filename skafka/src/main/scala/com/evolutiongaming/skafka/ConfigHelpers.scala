package com.evolutiongaming.skafka

import com.evolutiongaming.config.ConfigHelper.{ConfigOps, FromConf}
import com.typesafe.config.{Config, ConfigException}

import java.nio.file.Path
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, SECONDS, TimeUnit}
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.util.{Failure, Success, Try}

object ConfigHelpers {

  type Pair = (String, String)

  implicit val SecurityProtocolFromConf: FromConf[SecurityProtocol] = FromConf[SecurityProtocol] {
    (conf, path) =>
      val str   = conf.getString(path)
      val value = SecurityProtocol.Values.find { _.name equalsIgnoreCase str }
      value getOrElse {
        throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse SecurityProtocol from $str")
      }
  }

  implicit val FilePathFromConf: FromConf[Path] = FromConf[Path] { (conf, path) =>
    val str = conf.getString(path)
    Try(Path.of(str)) match {
      case Failure(exception) => throw new ConfigException.BadValue(conf.origin(), path, exception.getMessage)
      case Success(value)     => value
    }
  }

  implicit val ClassFromConf: FromConf[Class[_]] = FromConf[Class[_]] { (conf, path) =>
    val className = conf.getString(path)
    Try(Class.forName(className)) match {
      case Failure(_)     => throw new ConfigException.BadValue(conf.origin(), path, s"Class '$className' doesn't exist")
      case Success(value) => value
    }
  }

  implicit val JaasOptionsFromConf: FromConf[List[Pair]] = FromConf[List[Pair]] { (conf, path) =>
    conf
      .getObject("")
      .entrySet
      .asScala
      .map(entry => (entry.getKey, entry.getValue.render()))
      .toList
  }

  implicit class ConfigHelpersOps(val config: Config) {
    def getMillis(path: String, pathWithUnit: => String): Option[FiniteDuration] =
      getDuration(path, pathWithUnit, MILLISECONDS)

    def getSeconds(path: String, pathWithUnit: => String): Option[FiniteDuration] =
      getDuration(path, pathWithUnit, SECONDS)

    private def getDuration(path: String, pathWithUnit: => String, timeUnit: TimeUnit): Option[FiniteDuration] = {
      val value =
        try config.getOpt[FiniteDuration](path)
        catch { case _: ConfigException => None }
      value orElse config.getOpt[Long](pathWithUnit).map { Duration(_, timeUnit) }
    }
  }
}
