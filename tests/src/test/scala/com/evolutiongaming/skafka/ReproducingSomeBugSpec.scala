package com.evolutiongaming.skafka
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{IO, Resource, Sync}
import cats.implicits._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.skafka.FiberWithBlockingCancel._
import com.evolutiongaming.skafka.IOSuite._
import com.evolutiongaming.skafka.ReproducingSomeBugSpec.SafeConsumer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ReproducingSomeBugSpec extends AnyFunSuite with Matchers {
  test("mb we have a concurrency issue with skafka/fiber.cancel/resource combo") {
    val testStep = for {
      pollCounter       <- Ref.of[IO, Int](0).toResource
      pollReturnedSome  <- Deferred[IO, Unit].toResource
      pollReturnedSome1 <- Deferred[IO, Unit].toResource
      consumer          <- SafeConsumer.of[IO]("pollResult", 1, pollReturnedSome)
      poll = {
        for {
          _ <- IO.delay(println(s"${System.nanoTime()} gonna do poll"))
          _ <- pollCounter.update(_ + 1)
          r <- consumer.poll(10.millis).onError({ case e => IO(println(s"${System.nanoTime()} poll failed $e")) })
          _ <- if (r.isDefined) pollReturnedSome1.complete(()).handleError(_ => ()) else IO.unit
          _ <- IO.delay(println(s"${System.nanoTime()} poll completed"))
        } yield ()
      }
      _ <- {
        val x = for {
          _ <- poll.foreverM.backgroundAwaitExit.timeoutRelease(10.seconds)
        } yield ()
        x.use(_ => pollReturnedSome1.get.timeout(30.seconds))
      }.toResource
      numberOfPolls <- pollCounter.get.toResource
    } yield numberOfPolls

    val numberOfPolls = testStep.use(i => i.pure[IO]).unsafeRunSync()
    println(s"total number of poll attempts: $numberOfPolls")
  }
}

object ReproducingSomeBugSpec {

  class SafeConsumer[F[_]: Sync](unsafe: UnsafeConsumer, pollReturnedSome: Deferred[F, Unit]) {

    def poll(timeout: FiniteDuration): F[Option[String]] = {
      for {
        result <- Sync[F].delay(unsafe.poll(timeout.toMillis))
        _      <- if (result.isDefined) pollReturnedSome.complete(()).handleError(_ => ()) else ().pure[F]
      } yield result
    }

    def close: F[Unit] = Sync[F].delay(unsafe.close())
  }

  object SafeConsumer {
    def of[F[_]: Sync](
      pollResultValue: String,
      pollReturnsSomeAfterAttemptN: Int,
      pollReturnedSome: Deferred[F, Unit]
    ): Resource[F, SafeConsumer[F]] =
      Resource.make(
        Sync[F].delay(
          new SafeConsumer[F](new UnsafeConsumer(pollResultValue, pollReturnsSomeAfterAttemptN), pollReturnedSome)
        )
      )(_.close)
  }

  class UnsafeConsumer(pollResultValue: String, pollReturnsSomeStartingWithAttemptN: Int) {
    @volatile private var closed  = false
    @volatile private var attempt = 0

    def poll(timeoutMs: Long): Option[String] = {
      if (closed) throw new IllegalStateException("Consumer is closed")
      Thread.sleep(timeoutMs)
      attempt += 1
      if (attempt >= pollReturnsSomeStartingWithAttemptN) {
        pollResultValue.some
      } else {
        none
      }
    }

    def close(): Unit = {
      closed = true
    }
  }
}
