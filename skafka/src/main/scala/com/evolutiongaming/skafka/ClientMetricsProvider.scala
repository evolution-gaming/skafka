package com.evolutiongaming.skafka

import cats.effect.kernel.Sync
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.{Metric, MetricName}

import java.util
import scala.jdk.CollectionConverters._

private[skafka] trait ClientMetricsProvider[F[_]] {
  def get: Seq[ClientMetric[F]]
}
private[skafka] object ClientMetricsProvider {

  def apply[F[_]: Sync](consumer: Consumer[_, _]): ClientMetricsProvider[F] =
    new ClientMetricsProviderImpl(consumer.metrics())

  def apply[F[_]: Sync](producer: Producer[_, _]): ClientMetricsProvider[F] =
    new ClientMetricsProviderImpl[F](producer.metrics())

  private class ClientMetricsProviderImpl[F[_]: Sync](source: => util.Map[MetricName, _ <: Metric])
      extends ClientMetricsProvider[F] {

    def get: Seq[ClientMetric[F]] = source.asScala.values.toSeq.map { m =>
      val metricName = m.metricName()
      ClientMetric(
        metricName.name(),
        metricName.group(),
        metricName.description(),
        metricName.tags().asScala.toMap,
        Sync[F].delay(m.metricValue())
      )
    }

  }
}
