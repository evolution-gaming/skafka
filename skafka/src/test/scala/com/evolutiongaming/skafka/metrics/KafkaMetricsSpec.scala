package com.evolutiongaming.skafka.metrics

import cats.effect.IO
import com.evolutiongaming.skafka.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class KafkaMetricsSpec extends AsyncFunSuite with Matchers {

  test("make records consumer and producer metrics for a client") {
    val result = for {
      registry <- InMemoryCollectorRegistry.of[IO]
      state <- KafkaMetrics
        .make[IO](registry)
        .use { kafkaMetrics =>
          val consumerMetrics = kafkaMetrics.consumer("client")
          val producerMetrics = kafkaMetrics.producer("client")
          for {
            _     <- consumerMetrics.call("poll", "topic", 1.millis, success = true)
            _     <- producerMetrics.send("topic", 1.millis, bytes = 100)
            state <- registry.state
          } yield state
        }
    } yield {
      state.observations.keys.map(_.name).toSet should contain("skafka_consumer_latency")
      state.counters.keys.map(_.name).toSet should contain("skafka_consumer_results")
      state.observations.keys.map(_.name).toSet should contain("skafka_producer_latency")
      state.counters.keys.map(_.name).toSet should contain("skafka_producer_results")
    }
    result.run()
  }
}
