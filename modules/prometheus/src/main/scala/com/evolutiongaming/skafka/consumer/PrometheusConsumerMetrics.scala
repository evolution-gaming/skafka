package com.evolutiongaming.skafka.consumer

import cats.effect.Sync
import com.evolutiongaming.skafka.PrometheusHelper._
import com.evolutiongaming.skafka.{ClientId, Topic, TopicPartition}
import io.prometheus.client.{CollectorRegistry, Counter, Summary}

object PrometheusConsumerMetrics {

  type Prefix = String

  object Prefix {
    val Default: Prefix = "skafka_consumer"
  }

  def apply[F[_] : Sync, K, V](
    registry: CollectorRegistry,
    prefix: Prefix = Prefix.Default)
    (clientId: ClientId): Metrics[F] = {

    val callsCounter = Counter.build()
      .name(s"${ prefix }_calls")
      .help("Number of topic calls")
      .labelNames("client", "topic", "type")
      .register(registry)

    val resultCounter = Counter.build()
      .name(s"${ prefix }_results")
      .help("Topic call result: success or failure")
      .labelNames("client", "topic", "type", "result")
      .register(registry)

    val latencySummary = Summary.build()
      .name(s"${ prefix }_latency")
      .help("Topic call latency in seconds")
      .labelNames("client", "topic", "type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.05)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.005)
      .register(registry)

    val recordsSummary = Summary.build()
      .name(s"${ prefix }_poll_records")
      .help("Number of records per poll")
      .labelNames("client", "topic")
      .register(registry)

    val bytesSummary = Summary.build()
      .name(s"${ prefix }_poll_bytes")
      .help("Number of bytes per poll")
      .labelNames("client", "topic")
      .register(registry)

    val rebalancesCounter = Counter.build()
      .name(s"${ prefix }_rebalances")
      .help("Number of rebalances")
      .labelNames("client", "topic", "partition", "type")
      .register(registry)

    val listTopicsLatency = Summary.build()
      .name(s"${ prefix }_list_topics_latency")
      .help("List topics latency in seconds")
      .labelNames("client")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.05)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.005)
      .register(registry)

    val async = Sync[F]

    import async.delay

    new Metrics[F] {

      def call(name: String, topic: Topic, latency: Long, success: Boolean) = delay {
        val result = if (success) "success" else "failure"
        latencySummary
          .labels(clientId, topic, name)
          .observe(latency.toSeconds)
        resultCounter
          .labels(clientId, topic, name, result)
          .inc()
      }

      def poll(topic: Topic, bytes: Int, records: Int) = delay {
        recordsSummary
          .labels(clientId, topic)
          .observe(records.toDouble)
        bytesSummary
          .labels(clientId, topic)
          .observe(bytes.toDouble)
      }

      def count(name: String, topic: Topic) = delay {
        callsCounter
          .labels(clientId, topic, name)
          .inc()
      }

      def rebalance(name: String, topicPartition: TopicPartition) = delay {
        rebalancesCounter
          .labels(clientId, topicPartition.topic, topicPartition.partition.toString, name)
          .inc()
      }

      def listTopics(latency: Long) = delay {
        listTopicsLatency
          .labels(clientId)
          .observe(latency.toSeconds)
      }
    }
  }
}

