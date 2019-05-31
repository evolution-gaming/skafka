package com.evolutiongaming.skafka.producer

import cats.effect.Async
import com.evolutiongaming.skafka.PrometheusHelper._
import com.evolutiongaming.skafka.{ClientId, Topic}
import io.prometheus.client.{CollectorRegistry, Counter, Summary}

object PrometheusProducerMetrics {
  type Prefix = String

  object Prefix {
    val Default: Prefix = "skafka_producer"
  }


  def apply[F[_] : Async](
    registry: CollectorRegistry,
    prefix: Prefix = Prefix.Default): ClientId => Metrics[F] = {

    val latencySummary = Summary.build()
      .name(s"${ prefix }_latency")
      .help("Latency in seconds")
      .labelNames("client", "topic", "type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.05)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.005)
      .register(registry)

    val bytesSummary = Summary.build()
      .name(s"${ prefix }_bytes")
      .help("Message size in bytes")
      .labelNames("client", "topic")
      .register(registry)

    val resultCounter = Counter.build()
      .name(s"${ prefix }_results")
      .help("Result: success or failure")
      .labelNames("client", "topic", "result")
      .register(registry)

    val callLatency = Summary.build()
      .name(s"${ prefix }_call_latency")
      .help("Call latency in seconds")
      .labelNames("client", "type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.05)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.005)
      .register(registry)

    val callCount = Counter.build()
      .name(s"${ prefix }_calls")
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

      val async = Async[F]

      import async.delay

      new Metrics[F] {

        override def initTransactions(latency: Long) = delay {
          observeLatency("init_transactions", latency)
        }

        override val beginTransaction = delay {
          callCount
            .labels(clientId, "begin_transaction")
            .inc()
        }

        override def sendOffsetsToTransaction(latency: Long) = delay {
          observeLatency("send_offsets", latency)
        }

        override def commitTransaction(latency: Long) = delay {
          observeLatency("commit_transaction", latency)
        }

        override def abortTransaction(latency: Long) = delay {
          observeLatency("abort_transaction", latency)
        }

        override def send(topic: Topic, latency: Long, bytes: Int) = delay {
          sendMeasure(result = "success", topic = topic, latency = latency)
          bytesSummary
            .labels(clientId, topic)
            .observe(bytes.toDouble)
        }

        override def failure(topic: Topic, latency: Long) = delay {
          sendMeasure(result = "failure", topic = topic, latency = latency)
        }

        override def partitions(topic: Topic, latency: Long) = delay {
          latencySummary
            .labels(clientId, topic, "partitions")
            .observe(latency.toSeconds)
        }

        override def flush(latency: Long) = delay {
          observeLatency("flush", latency)
        }

        override def close(latency: Long) = delay {
          observeLatency("close", latency)
        }
      }
    }
  }
}
