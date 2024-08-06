package com.evolutiongaming.skafka

import cats.effect._
import cats.syntax.all._
import cats.effect.testkit.TestControl
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.IOSuite._
import com.evolutiongaming.skafka.KafkaHealthCheck.Record
import com.evolutiongaming.skafka.Topic
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class KafkaHealthCheckSpec extends AsyncFunSuite with Matchers {
  import KafkaHealthCheckSpec._

  test("error") {
    implicit val log = Log.empty[IO]

    val producer = new KafkaHealthCheck.Producer[IO] {
      def send(record: Record) = ().pure[IO]
    }

    val consumer = new KafkaHealthCheck.Consumer[IO] {

      def subscribe(topic: Topic) = ().pure[IO]

      def poll(timeout: FiniteDuration) = {
        if (timeout == 1.second) List.empty[Record].pure[IO]
        else Error.raiseError[IO, List[Record]]
      }
    }

    val healthCheck = KafkaHealthCheck.of[IO](
      key      = "key",
      config   = KafkaHealthCheck.Config(topic = "topic", initial = 0.seconds, interval = 1.second),
      stop     = false.pure[IO],
      producer = Resource.pure[IO, KafkaHealthCheck.Producer[IO]](producer),
      consumer = Resource.pure[IO, KafkaHealthCheck.Consumer[IO]](consumer),
      log      = log
    )
    val result = for {
      error <- healthCheck.use(_.error.untilDefinedM)
    } yield {
      error shouldEqual error
    }
    result.run()
  }

  test("periodic healthcheck") {
    final case class State(
      checks: Int               = 0,
      subscribed: Option[Topic] = None,
      logs: List[String]        = List.empty,
      records: List[Record]     = List.empty
    )

    def logOf(ref: Ref[IO, State]): Log[IO] = {
      def add(log: String): IO[Unit] =
        ref.update(state => state.copy(logs = log :: state.logs))

      new Log[IO] {
        def trace(msg: => String, mdc: Log.Mdc) = add(s"trace $msg")

        def debug(msg: => String, mdc: Log.Mdc) = add(s"debug $msg")

        def info(msg: => String, mdc: Log.Mdc) = add(s"info $msg")

        def warn(msg: => String, mdc: Log.Mdc) = add(s"warn $msg")

        def warn(msg: => String, cause: Throwable, mdc: Log.Mdc) = add(s"warn $msg $cause")

        def error(msg: => String, mdc: Log.Mdc) = add(s"error $msg")

        def error(msg: => String, cause: Throwable, mdc: Log.Mdc) = add(s"error $msg $cause")
      }
    }

    def consumerOf(ref: Ref[IO, State]) = new KafkaHealthCheck.Consumer[IO] {
      def subscribe(topic: Topic): IO[Unit] =
        ref.update(_.copy(subscribed = topic.some))

      def poll(timeout: FiniteDuration): IO[Iterable[Record]] =
        ref
          .modify(state =>
            if (state.records.size >= 2) (state.copy(records = List.empty), state.records)
            else (state, List.empty)
          )
    }

    def producerOf(ref: Ref[IO, State]): KafkaHealthCheck.Producer[IO] = new KafkaHealthCheck.Producer[IO] {
      def send(record: Record): IO[Unit] =
        ref.update(state => state.copy(records = record :: state.records))
    }

    def stopOf(ref: Ref[IO, State]): IO[Boolean] =
      ref.updateAndGet(state => state.copy(checks = state.checks - 1)).map(_.checks <= 0)

    val result = for {
      ref <- Ref.of[IO, State](State(checks = 2))
      healthCheck = KafkaHealthCheck.of[IO](
        key = "key",
        config =
          KafkaHealthCheck.Config(topic = "topic", initial = 0.millis, interval = 0.millis, timeout = 100.millis),
        stop     = stopOf(ref),
        producer = Resource.pure[IO, KafkaHealthCheck.Producer[IO]](producerOf(ref)),
        consumer = Resource.pure[IO, KafkaHealthCheck.Consumer[IO]](consumerOf(ref)),
        log      = logOf(ref)
      )
      _     <- TestControl.executeEmbed(healthCheck.use(_.done))
      state <- ref.get

    } yield state shouldEqual State(
      checks     = 0,
      subscribed = "topic".some,
      logs = List(
        "debug key send 2:0",
        "debug key send 2",
        "debug key send 1:0",
        "debug key send 1",
        "debug key send 0:0",
        "debug key send 0"
      ),
      records = List()
    )

    result.run()
  }
}

object KafkaHealthCheckSpec {
  val Error: Throwable = new RuntimeException with NoStackTrace
}
