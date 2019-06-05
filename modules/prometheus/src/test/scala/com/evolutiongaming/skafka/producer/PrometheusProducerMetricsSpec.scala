package com.evolutiongaming.skafka
package producer

import cats.effect.IO
import cats.implicits._
import com.evolutiongaming.skafka.IOMatchers._
import com.evolutiongaming.skafka.IOSuite._
import io.prometheus.client.CollectorRegistry
import org.scalatest.{Matchers, WordSpec}

class PrometheusProducerMetricsSpec extends WordSpec with Matchers {

  "PrometheusProducerMetrics" should {

    "measure initTransactions" in new Scope {
      verify(producer.initTransactions) { _ =>
        callLatencyCount("init_transactions") shouldEqual Some(1.0)
      }
    }

    "measure beginTransaction" in new Scope {
      verify(producer.beginTransaction) { _ =>
        callCount("begin_transaction") shouldEqual Some(1.0)
      }
    }

    "measure sendOffsetsToTransaction" in new Scope {
      verify(producer.sendOffsetsToTransaction(Map.empty, "consumerGroupId")) { _ =>
        callLatencyCount("send_offsets") shouldEqual Some(1.0)
      }
    }

    "measure commitTransaction" in new Scope {
      verify(producer.commitTransaction) { _ =>
        callLatencyCount("commit_transaction") shouldEqual Some(1.0)
      }
    }

    "measure abortTransaction" in new Scope {
      verify(producer.abortTransaction) { _ =>
        callLatencyCount("abort_transaction") shouldEqual Some(1.0)
      }
    }

    "measure send call" in new Scope {

      val record = ProducerRecord(topic = "topic", key = "key", value = "val")
      private val io = producer.send(record).flatten
      verify(io) { _ =>
        def result(name: String) = Option {
          registry.getSampleValue(
            "skafka_producer_results",
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

        latencyCount("send") shouldEqual Some(1.0)
        latencyCount("block") shouldEqual Some(1.0)
      }
    }

    "measure partitions" in new Scope {
      verify(producer.partitions("topic")) { _ =>
        latencyCount("partitions") shouldEqual Some(1.0)
      }
    }

    "measure flush" in new Scope {
      verify(producer.flush) { _ =>
        callLatencyCount("flush") shouldEqual Some(1.0)
      }
    }
  }

  private trait Scope {
    val registry = new CollectorRegistry()
    val metrics = PrometheusProducerMetrics[IO](registry).apply("")
    val producer = Producer[IO](Producer.empty[IO], metrics)

    def callLatencyCount(name: String) = Option {
      registry.getSampleValue(
        "skafka_producer_call_latency_count",
        Array("client", "type"),
        Array("", name))
    }

    def callCount(name: String) = Option {
      registry.getSampleValue(
        "skafka_producer_calls",
        Array("client", "type"),
        Array("", name))
    }

    def latencyCount(name: String) = Option {
      registry.getSampleValue(
        "skafka_producer_latency_count",
        Array("client", "topic", "type"),
        Array("", "topic", name))
    }
  }
}

