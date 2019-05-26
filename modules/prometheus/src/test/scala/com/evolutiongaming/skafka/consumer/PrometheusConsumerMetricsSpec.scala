package com.evolutiongaming.skafka
package consumer

import cats.effect.IO
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.IOMatchers._
import io.prometheus.client.CollectorRegistry
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

class PrometheusConsumerMetricsSpec extends WordSpec with Matchers {

  val clientId = "clientId"
  val topic = "topic"

  "PrometheusConsumerMetrics" should {

    "measure assign" in new Scope {
      val topicPartition = TopicPartition(topic = topic, Partition.Min)
      verify(consumer.assign(Nel(topicPartition))) { _ =>
        registry.count("assign") shouldEqual Some(1.0)
      }
    }

    "measure listTopics" in new Scope {
      consumer.listTopics should produce(Map.empty[Topic, List[PartitionInfo]])

      def result(name: String) = Option {
        registry.getSampleValue(
          s"skafka_consumer_list_topics_latency_$name",
          Array("client"),
          Array(clientId))
      }

      result("count") shouldEqual Some(1.0)
      result("sum").isDefined shouldEqual true
    }

    "measure poll" in new Scope {
      verify(consumer.poll(1.second)) { _ =>
        registry.latencyCount("poll") shouldEqual None
      }
    }

    "measure commit" in new Scope {
      val topicPartition = TopicPartition(topic = topic, Partition.Min)

      verify(consumer.commit(Map((topicPartition, OffsetAndMetadata.Empty)))) { _ =>

        def result(name: String) = Option {
          registry.getSampleValue(
            "skafka_consumer_results",
            Array("client", "topic", "type", "result"),
            Array("clientId", topicPartition.topic, "commit", name))
        }

        registry.latencySum("commit").isDefined shouldEqual true
        registry.latencyCount("commit") shouldEqual Some(1.0)

        result("success") shouldEqual Some(1.0)
        result("failure") shouldEqual None
      }
    }

    "measure subscribe" in new Scope {
      verify(consumer.subscribe(Nel(topic), Some(RebalanceListener.Empty))) { _ =>
        registry.count("subscribe") shouldEqual Some(1.0)
      }
    }
  }

  private trait Scope {
    implicit val ec = CurrentThreadExecutionContext
    implicit val cs = IO.contextShift(ec)
    val registry = new CollectorRegistry()
    val metrics = PrometheusConsumerMetrics[IO, String, String](registry)(clientId)
    val consumer = Consumer(Consumer.empty[IO, String, String], metrics)
  }

  implicit class CollectorRegistryOps(registry: CollectorRegistry) {

    def latencyCount(name: String) = Option {
      registry.getSampleValue(
        s"skafka_consumer_latency_count",
        Array("client", "topic", "type"),
        Array(clientId, topic, name))
    }

    def latencySum(name: String) = Option {
      registry.getSampleValue(
        s"skafka_consumer_latency_sum",
        Array("client", "topic", "type"),
        Array(clientId, topic, name))
    }

    def count(name: String) = Option {
      registry.getSampleValue(
        "skafka_consumer_calls",
        Array("client", "topic", "type"),
        Array(clientId, topic, name))
    }
  }
}
