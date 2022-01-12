package com.evolutiongaming.skafka

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.evolutiongaming.catshelper.Blocking
import org.scalatest.Succeeded

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object IOSuite {
  val Timeout: FiniteDuration = 1.minute

  implicit val executor: ExecutionContextExecutor = ExecutionContext.global

  implicit val blocking: Blocking[IO] = Blocking.empty[IO]
  implicit val ioRuntime: IORuntime   = IORuntime.global

  def runIO[A](io: IO[A], timeout: FiniteDuration = Timeout): Future[Succeeded.type] = {
    io.timeout(timeout).as(Succeeded).unsafeToFuture()
  }

  implicit class IOOps[A](val self: IO[A]) extends AnyVal {
    def run(timeout: FiniteDuration = Timeout): Future[Succeeded.type] = runIO(self, timeout)
  }
}
