package com.evolutiongaming.skafka

object PrometheusHelper {

  implicit class LongOps(val self: Long) extends AnyVal {
    def toSeconds: Double = self.toDouble / 1000
  }
}