package com.evolutiongaming.skafka

import cats.Parallel
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.FromFuture
import org.scalatest.Succeeded

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object IOSuite {
  val Timeout: FiniteDuration = 10.seconds

  implicit val executor: ExecutionContextExecutor = ExecutionContext.global

  implicit val contextShiftIO: ContextShift[IO] = IO.contextShift(executor)
  implicit val concurrentIO: Concurrent[IO]     = IO.ioConcurrentEffect
  implicit val timerIO: Timer[IO]               = IO.timer(executor)
  implicit val parallelIO: Parallel[IO, IO.Par] = IO.ioParallel
  implicit val fromFutureIO: FromFuture[IO]     = FromFuture.lift[IO]

  def runIO[A](io: IO[A], timeout: FiniteDuration = Timeout): Future[Succeeded.type] = {
    io.timeout(timeout).as(Succeeded).unsafeToFuture
  }

  implicit class IOOps[A](val self: IO[A]) extends AnyVal {
    def run(timeout: FiniteDuration = Timeout): Future[Succeeded.type] = runIO(self, timeout)
  }
}
