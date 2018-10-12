package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.PrometheusHelper._
import com.evolutiongaming.skafka.{ClientId, Topic}
import io.prometheus.client.{CollectorRegistry, Counter, Summary}

object PrometheusProducerMetrics {
  type Prefix = String

  object Prefix {
    val Default: Prefix = "skafka_producer"
  }


  def apply(
    registry: CollectorRegistry,
    prefix: Prefix = Prefix.Default): ClientId => Producer.Metrics = {

    val latencySummary = Summary.build()
      .name(s"${ prefix }_latency")
      .help("Latency in seconds")
      .labelNames("client", "topic", "type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .register(registry)

    val bytesSummary = Summary.build()
      .name(s"${ prefix }_bytes")
      .help("Message size in bytes")
      .labelNames("client", "topic")
      .register(registry)

    val resultCounter = Counter.build()
      .name(s"${ prefix }_result")
      .help("Result: success or failure")
      .labelNames("client", "topic", "result")
      .register(registry)

    val callLatency = Summary.build()
      .name(s"${ prefix }_call_latency")
      .help("Call latency in seconds")
      .labelNames("client", "type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .register(registry)

    val callCount = Counter.build()
      .name(s"${ prefix }_call_count")
      .help("Call count")
      .labelNames("client", "type")
      .register(registry)

    clientId: ClientId => {

      def sendMeasure(result: String, topic: Topic, latency: Long) = {
        latencySummary
          .labels(clientId, topic, "send")
          .observe(latency.toSeconds)
        resultCounter
          .labels(clientId, topic, result)
          .inc()
      }

      def observeLatency(name: String, latency: Long) = {
        callLatency
          .labels(clientId, name)
          .observe(latency.toSeconds)
      }

      new Producer.Metrics {

        def initTransactions(latency: Long) = {
          observeLatency("init_transactions", latency)
        }

        def beginTransaction() = {
          callCount
            .labels(clientId, "begin_transaction")
            .inc()
        }

        def sendOffsetsToTransaction(latency: Long) = {
          observeLatency("send_offsets", latency)
        }

        def commitTransaction(latency: Long) = {
          observeLatency("commit_transaction", latency)
        }

        def abortTransaction(latency: Long) = {
          observeLatency("abort_transaction", latency)
        }

        def send(topic: Topic, latency: Long, bytes: Int) = {
          sendMeasure(result = "success", topic = topic, latency = latency)
          bytesSummary
            .labels(clientId, topic)
            .observe(bytes.toDouble)
        }

        def failure(topic: Topic, latency: Long) = {
          sendMeasure(result = "failure", topic = topic, latency = latency)
        }

        def partitions(topic: Topic, latency: Long) = {
          latencySummary
            .labels(clientId, topic, "partitions")
            .observe(latency.toSeconds)
        }

        def flush(latency: Long): Unit = {
          observeLatency("flush", latency)
        }

        def close(latency: Long) = {
          observeLatency("close", latency)
        }
      }
    }
  }
}
