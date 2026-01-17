package com.evolutiongaming.skafka

import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.{Config, ConfigException}
import org.apache.kafka.clients.{CommonClientConfigs => C}

import scala.concurrent.duration._

final case class MetricsConfig(
  sampleWindow: FiniteDuration = 30.seconds,
  numSamples: Int              = 2,
  recordingLevel: String       = "INFO",
  reporters: List[String]      = Nil
) {

  def bindings: Map[String, String] = Map[String, String](
    (C.METRICS_SAMPLE_WINDOW_MS_CONFIG, sampleWindow.toMillis.toString),
    (C.METRICS_NUM_SAMPLES_CONFIG, numSamples.toString),
    (C.METRICS_RECORDING_LEVEL_CONFIG, recordingLevel),
    (C.METRIC_REPORTER_CLASSES_CONFIG, reporters mkString ",")
  )
}

object MetricsConfig {

  val Default: MetricsConfig = MetricsConfig()

  def apply(config: Config): MetricsConfig = {

    def get[T: FromConf](path: String, paths: String*) = {
      config.getOpt[T](path, paths: _*)
    }

    def getDuration(path: String, pathMs: => String) = {
      val value =
        try get[FiniteDuration](path)
        catch { case _: ConfigException => None }
      value orElse get[Long](pathMs).map { _.millis }
    }

    MetricsConfig(
      sampleWindow   = getDuration("metrics.sample-window", "metrics.sample.window.ms") getOrElse Default.sampleWindow,
      numSamples     = get[Int]("metrics.num-samples", "metrics.num.samples") getOrElse Default.numSamples,
      recordingLevel =
        get[String]("metrics.recording-level", "metrics.recording.level") getOrElse Default.recordingLevel,
      reporters = get[List[String]]("metrics.reporters", "metric.reporters") getOrElse Default.reporters
    )
  }
}
