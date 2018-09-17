package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.{Partition, TopicPartition}
import io.prometheus.client.CollectorRegistry
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Future
import scala.util.Success

class PrometheusConsumerMetricsSpec extends WordSpec with Matchers {

  "PrometheusConsumerMetrics" should {

    "measure listTopics" in {
      val registry = new CollectorRegistry()
      val metrics = PrometheusConsumerMetrics(registry)
      val consumer = Consumer(Consumer.empty[String, String], metrics)
      consumer.listTopics().value shouldEqual Some(Success(Map.empty))

      def result(name: String) = Option {
        registry.getSampleValue(
          s"skafka_consumer_list_topics_latency_$name",
          Array("client"),
          Array(""))
      }

      result("count") shouldEqual Some(1.0)
      result("sum").isDefined shouldEqual true
    }

    "measure commit" in {
      val registry = new CollectorRegistry()
      val topicPartition = TopicPartition(topic = "topic", Partition.Min)
      val metrics = PrometheusConsumerMetrics(registry, clientId = "clientId")
      val consumer = Consumer(Consumer.empty[String, String], metrics)
      consumer.commit(Map((topicPartition, OffsetAndMetadata.Empty))) shouldEqual Future.unit

      def result(name: String) = Option {
        registry.getSampleValue(
          "skafka_consumer_results",
          Array("client", "topic", "type", "result"),
          Array("clientId", topicPartition.topic, "commit", name))
      }

      def latency(name: String) = Option {
        registry.getSampleValue(
          s"skafka_consumer_latency_$name",
          Array("client", "topic", "type"),
          Array("clientId", topicPartition.topic, "commit"))
      }

      latency("sum").isDefined shouldEqual true
      latency("count") shouldEqual Some(1.0)

      result("success") shouldEqual Some(1.0)
      result("failure") shouldEqual None
    }

    "measure subscribe" in {
      val registry = new CollectorRegistry()
      val topic = "topic"
      val metrics = PrometheusConsumerMetrics(registry, "consumer")
      val consumer = Consumer(Consumer.empty[String, String], metrics)
      consumer.subscribe(Nel(topic), Some(RebalanceListener.Empty))

      val count = Option {
        registry.getSampleValue(
          "consumer_calls",
          Array("client", "topic", "type"),
          Array("", topic, "subscribe"))
      }
      count shouldEqual Some(1.0)
    }
  }
}
