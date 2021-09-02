package com.evolutiongaming.skafka

import com.evolutiongaming.config.ConfigHelper.ConfigOps
import com.typesafe.config.{Config, ConfigException}

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, SECONDS, TimeUnit}

object ConfigHelpers {

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
