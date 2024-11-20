package com.evolutiongaming.skafka.metrics

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.ClientMetric
import io.prometheus.client.Collector
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.Collector.MetricFamilySamples.Sample

import java.util
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

/** Prometheus collector for Kafka client metrics.
  *
  * Metrics from kafka-clients are pull-based (meaning we have to manually call methods that return metrics), this
  * doesn't allow us to use the default push-based approach of Prometheus with passing an instance of
  * `CollectorRegistry` to the code being observed.
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
  * val collectorRegistry: CollectorRegistry = ??? // your Prometheus Java collector registry
  * val consumer: Consumer[F, K, V]          = ??? // your consumer
  * val collector: KafkaMetricsCollector[F]  = new KafkaMetricsCollector[F](consumer.clientMetrics)
  * collectorRegistry.register(collector)
  * }}}
  */
class KafkaMetricsCollector[F[_]: Monad: ToTry](
  kafkaClientMetrics: F[Seq[ClientMetric[F]]],
  prefix: Option[String]               = None,
  customLabels: List[(String, String)] = List.empty,
) extends Collector {

  protected def getCollectorType(metric: ClientMetric[F]): Collector.Type = {
    // https://prometheus.io/docs/practices/naming/#metric-names
    if (metric.name.endsWith("total")) Collector.Type.COUNTER
    else if (metric.name.contains("count")) Collector.Type.SUMMARY
    else Collector.Type.GAUGE
  }

  private val MetricNameRegex: Regex = "[a-zA-Z_:][a-zA-Z0-9_:]*".r
  private val LabelNameRegex: Regex  = "[a-zA-Z_][a-zA-Z0-9_]*".r

  protected def getPrometheusName(metric: ClientMetric[F]): Option[String] = {
    (metric.name, metric.description) match {
      case (name, description) if name.nonEmpty && description.nonEmpty =>
        val prometheusName =
          (prefix.toList :+ metric.group :+ metric.name).mkString("_").replaceAll("-", "_")

        if (MetricNameRegex.findFirstIn(prometheusName).contains(prometheusName)) prometheusName.some else None

      case _ => None
    }
  }

  override def collect(): util.List[MetricFamilySamples] = {
    for {
      metrics      <- kafkaClientMetrics
      metricsGroups = metrics.groupBy(m => (m.name, m.group)).values.toList
      result <- metricsGroups
        .traverse { metricsGroup =>
          val metric         = metricsGroup.head
          val prometheusName = getPrometheusName(metric)
          val collectorType  = getCollectorType(metric)

          prometheusName match {
            case Some(name) =>
              metricsGroup
                .toVector
                .traverse { metric =>
                  val tags = metric.tags.flatMap {
                    case (key, value) =>
                      val prometheusKey = key.replaceAll("-", "_")
                      if (LabelNameRegex.findFirstIn(prometheusKey).contains(prometheusKey))
                        (prometheusKey -> value).some
                      else None
                  }
                  val (customLabelsKeys, customLabelsValues) = customLabels.separate
                  val tagsKeys                               = (tags.keys.toList ++ customLabelsKeys).asJava
                  val tagsValues                             = (tags.values.toList ++ customLabelsValues).asJava
                  metric.value.map {
                    case v: Number =>
                      new Sample(name, tagsKeys, tagsValues, v.doubleValue()).some

                    case _ => Option.empty[Sample]
                  }
                }
                .map(_.flatten)
                .map {
                  case samples if samples.nonEmpty =>
                    new MetricFamilySamples(name, collectorType, metric.description, samples.asJava).some

                  case _ => Option.empty[MetricFamilySamples]
                }

            case None => Option.empty[MetricFamilySamples].pure
          }
        }
        .map(_.flatten.asJava)
    } yield result
  }.toTry.get
}
