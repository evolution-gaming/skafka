package com.evolution.skafka.metrics

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.ClientMetric

import scala.jdk.CollectionConverters._
import io.prometheus.metrics.model.registry.MultiCollector
import io.prometheus.metrics.model.snapshots._

import KafkaMetricsCollector._
import scala.util.Try

/** Prometheus collector for Kafka client metrics.
  *
  * Metrics from kafka-clients are pull-based (meaning we have to manually call methods that return metrics), this
  * doesn't allow us to use the default push-based approach of Prometheus with passing an instance of
  * `PrometheusRegistry` to the code being observed.
  *
  * This class is a workaround for this problem. It's a Prometheus collector that evaluates a given
  * `F[Seq[ClientMetric[F]]]` on each `collect()` call and returns the result as a list of `MetricFamilySamples`.
  *
  * Please note it ignores metrics and labels containing some symbols (like '-' and '.'). This is because Prometheus
  * doesn't allow such symbols in metric names and label names. Also, it ignores metrics without a name or a
  * description. See [[https://prometheus.io/docs/practices/naming/]] for more details.
  *
  * Example:
  * {{{
  * val collectorRegistry: PrometheusRegistry = ??? // your Prometheus Java collector registry
  * val consumer: Consumer[F, K, V]          = ??? // your consumer
  * val collector: KafkaMetricsCollector[F]  = new KafkaMetricsCollector[F](consumer.clientMetrics)
  * collectorRegistry.register(collector)
  * }}}
  */
class KafkaMetricsCollector[F[_]: Monad: ToTry](
  kafkaClientMetrics: F[Seq[ClientMetric[F]]],
  prefix: Option[String] = None,
  customLabels: List[(String, String)],
) extends MultiCollector {

  private val (customLabelsKeys, customLabelsValues) = customLabels.separate

  override def collect(): MetricSnapshots = {
    for {
      metrics      <- kafkaClientMetrics
      metricsGroups = metrics.groupBy(m => (m.name, m.group)).toList
      result <- metricsGroups
        .traverse {
          case ((name, group), metricsGroup) =>
            val description = metricsGroup.head.description
            getPrometheusName(name = name, group = group, description = description) match {
              case Some(name) =>
                metricsGroup
                  .toVector
                  .traverse(buildSample(_))
                  .map(_.flatten)
                  .map {
                    case samples if samples.nonEmpty =>
                      Some(buildSnapshot(metricName = name, description = description, samples = samples))

                    case _ => Option.empty[MetricSnapshot]
                  }

              case None => Option.empty[MetricSnapshot].pure
            }
        }
        .map(_.flatten.asJava)
    } yield new MetricSnapshots(result)
  }.toTry.get

  protected def getPrometheusName(name: String, group: String, description: String): Option[String] = {
    if (name.nonEmpty && description.nonEmpty) {
      val prometheusName = (prefix.toList :+ group :+ name).mkString("_")
      Try(PrometheusNaming.sanitizeMetricName(prometheusName)).toOption
    } else None
  }

  private def buildSnapshot(metricName: String, description: String, samples: Vector[MetricSample]): MetricSnapshot =
    if (metricName.endsWith("total")) buildCounterSnapshot(metricName, description, samples)
    else buildGaugeSnapshot(metricName, description, samples)

  private def buildCounterSnapshot(
    metricName: String,
    description: String,
    samples: Vector[MetricSample]
  ): MetricSnapshot = {
    val snapshotBuilder = CounterSnapshot.builder()
    samples.foreach { sample =>
      val dataPoint =
        CounterSnapshot.CounterDataPointSnapshot.builder().value(sample.value).labels(sample.labels).build()
      snapshotBuilder.dataPoint(dataPoint)
    }
    snapshotBuilder.name(metricName).help(description).build()
  }

  private def buildGaugeSnapshot(
    metricName: String,
    description: String,
    samples: Vector[MetricSample]
  ): MetricSnapshot = {
    val snapshotBuilder = GaugeSnapshot.builder()
    samples.foreach { sample =>
      val dataPoint =
        GaugeSnapshot.GaugeDataPointSnapshot.builder().value(sample.value).labels(sample.labels).build()
      snapshotBuilder.dataPoint(dataPoint)
    }
    snapshotBuilder.name(metricName).help(description).build()
  }

  private def buildSample(metric: ClientMetric[F]): F[Option[MetricSample]] = {
    val tags = metric.tags.flatMap {
      case (key, value) =>
        Try(PrometheusNaming.sanitizeLabelName(key)).toOption.map(_ -> value)
    }
    val tagsKeys   = (tags.keys.toList ++ customLabelsKeys)
    val tagsValues = (tags.values.toList ++ customLabelsValues)
    metric.value.map {
      case v: Number =>
        MetricSample(tagsKeys, tagsValues, v.doubleValue()).some
      case _ =>
        none[MetricSample]
    }
  }

  override def getPrometheusNames(): java.util.List[String] =
    getMetricNames().toTry.getOrElse(List.empty).asJava

  private def getMetricNames(): F[List[String]] = {
    kafkaClientMetrics.map { metrics =>
      val metricsGroups = metrics.groupBy(m => (m.name, m.group)).toList
      metricsGroups.flatMap { case ((name, group), metricsGroup) =>
        val description = metricsGroup.head.description
        getPrometheusName(name, group, description)
      }.distinct
    }
  }

}

object KafkaMetricsCollector {
  private final case class MetricSample(
    tagNames: List[String],
    tagValues: List[String],
    value: Double,
  ) {
    def labels: Labels = Labels.of(tagNames.asJava, tagValues.asJava)
  }
}
