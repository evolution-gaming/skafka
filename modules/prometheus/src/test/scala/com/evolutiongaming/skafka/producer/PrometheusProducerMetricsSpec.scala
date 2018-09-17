package com.evolutiongaming.skafka.producer

import io.prometheus.client.CollectorRegistry
import org.scalatest.{Matchers, WordSpec}

class PrometheusProducerMetricsSpec extends WordSpec with Matchers {

  "PrometheusProducerMetrics" should {

    "measure send call" in {

      val registry = new CollectorRegistry()
      val metrics = PrometheusProducerMetrics(registry)
      val producer = Producer(Producer.Empty, metrics)

      val record = ProducerRecord(topic = "topic", key = "key", value = "val")
      producer.send(record)

      def result(name: String) = Option {
        registry.getSampleValue(
          "skafka_producer_result",
          Array("client", "topic", "result"),
          Array("", "topic", name))
      }

      def bytes(name: String) = Option {
        registry.getSampleValue(
          s"skafka_producer_bytes_$name",
          Array("client", "topic"),
          Array("", "topic"))
      }

      result("success") shouldEqual Some(1.0)
      result("failure") shouldEqual None

      bytes("count") shouldEqual Some(1.0)
      bytes("sum") shouldEqual Some(0.0)
    }
  }
}

