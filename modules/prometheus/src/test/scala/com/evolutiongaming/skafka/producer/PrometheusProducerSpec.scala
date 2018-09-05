package com.evolutiongaming.skafka.producer

import io.prometheus.client.CollectorRegistry
import org.scalatest.{Matchers, WordSpec}

class PrometheusProducerSpec extends WordSpec with Matchers {

  "PrometheusProducer" should {

    "create twice for same label" in {
      val registry = new CollectorRegistry()

      def create() = PrometheusProducer(Producer.Empty, registry)

      create()
      create()
    }

    "measure send call" in {
      val registry = new CollectorRegistry()
      val producer = PrometheusProducer(Producer.Empty, registry)
      val record = ProducerRecord(topic = "topic", key = "key", value = "val")
      producer.send(record)

      def result(name: String) = Option {
        registry.getSampleValue(
          "skafka_producer_result",
          Array("producer", "topic", "result"),
          Array("", "topic", name))
      }

      def bytes(name: String) = Option {
        registry.getSampleValue(
          s"skafka_producer_bytes_$name",
          Array("producer", "topic"),
          Array("", "topic"))
      }

      result("success") shouldEqual Some(1.0)
      result("failure") shouldEqual None

      bytes("count") shouldEqual Some(1.0)
      bytes("sum") shouldEqual Some(0.0)
    }
  }
}
