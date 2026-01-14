package com.evolution.skafka.metrics

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.evolutiongaming.catshelper.ToTry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class KafkaMetricsRegistrySpec extends AnyFunSuite with Matchers {

  implicit val toTry: ToTry[IO] = ToTry.ioToTry

  test("allows single registration per PrometheusRegistry and prefix") {
    val prometheus = new PrometheusRegistry()

    val result = KafkaMetricsRegistry
      .of[IO](prometheus, Some("kafka"))
      .use { registry =>
        IO.pure(registry)
      }
      .unsafeRunSync()

    result should not be null
  }

  test("throws IllegalStateException when registering twice with same parameters") {
    val prometheus = new PrometheusRegistry()
    val prefix     = Some("kafka")

    val result = (for {
      registry1 <- KafkaMetricsRegistry.of[IO](prometheus, prefix)
      registry2 <- KafkaMetricsRegistry.of[IO](prometheus, prefix)
    } yield (registry1, registry2))
      .use(_ => IO.unit)
      .attempt
      .unsafeRunSync()

    result match {
      case Left(e: IllegalStateException) =>
        e.getMessage should include("KafkaMetricsRegistry already registered")
        e.getMessage should include("prefix=Some(kafka)")
      case other => fail(s"Expected IllegalStateException, got: $other")
    }
  }

  test("allows registration after resource is released") {
    val prometheus = new PrometheusRegistry()
    val prefix     = Some("kafka")

    val result1 = KafkaMetricsRegistry
      .of[IO](prometheus, prefix)
      .use { registry =>
        IO.pure(registry)
      }
      .unsafeRunSync()

    result1 should not be null

    val result2 = KafkaMetricsRegistry
      .of[IO](prometheus, prefix)
      .use { registry =>
        IO.pure(registry)
      }
      .unsafeRunSync()

    result2 should not be null
  }

  test("allows simultaneous registrations with different prefixes") {
    val prometheus = new PrometheusRegistry()

    val result = (for {
      registry1 <- KafkaMetricsRegistry.of[IO](prometheus, Some("kafka1"))
      registry2 <- KafkaMetricsRegistry.of[IO](prometheus, Some("kafka2"))
    } yield (registry1, registry2))
      .use { case (r1, r2) =>
        IO.pure((r1, r2))
      }
      .unsafeRunSync()

    result._1 should not be null
    result._2 should not be null
  }

  test("allows simultaneous registrations with different PrometheusRegistry instances") {
    val prometheus1 = new PrometheusRegistry()
    val prometheus2 = new PrometheusRegistry()

    val result = (for {
      registry1 <- KafkaMetricsRegistry.of[IO](prometheus1, Some("kafka"))
      registry2 <- KafkaMetricsRegistry.of[IO](prometheus2, Some("kafka"))
    } yield (registry1, registry2))
      .use { case (r1, r2) =>
        IO.pure((r1, r2))
      }
      .unsafeRunSync()

    result._1 should not be null
    result._2 should not be null
  }

}
