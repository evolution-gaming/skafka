package com.evolutiongaming.skafka

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import io.prometheus.client.Summary

import scala.compat.Platform
import scala.concurrent.Future

object PrometheusHelper {

  implicit class LongOps(val self: Long) extends AnyVal {
    def toSeconds: Double = self.toDouble / 1000
  }

  implicit class SummaryOps(val self: Summary.Child) extends AnyVal {

    def time[T](f: => Future[T]): Future[T] = {
      implicit val ec = CurrentThreadExecutionContext
      val time = Platform.currentTime
      val result = f
      result.onComplete { _ =>
        val duration = (Platform.currentTime - time).toSeconds
        self.observe(duration)
        result
      }
      result
    }
  }
}