package com.evolutiongaming.skafka

import cats.Applicative
import cats.effect.kernel.{Deferred, Temporal}
import cats.implicits._
import cats.effect.syntax.all._
import cats.effect.{Concurrent, Fiber, Resource}

import scala.concurrent.duration.FiniteDuration

object FiberWithBlockingCancel {
  implicit class FiberWithBlockingCancelOps[F[_], A](val self: F[A]) extends AnyVal {
    def startAwaitExit(implicit c: Concurrent[F]): F[Fiber[F, Throwable, A]] = {
      for {
        deferred <- Deferred[F, Unit]
        fiber    <- self.guarantee {
          deferred.complete(()).handleError { _ => true }.void
        }.start
      } yield {
        new Fiber[F, Throwable, A] {
          def cancel = {
            for {
              _ <- fiber.cancel
              _ <- deferred.get
            } yield {}
          }

          def join = fiber.join
        }
      }
    }

    def backgroundAwaitExit(implicit c: Concurrent[F]): Resource[F, Unit] = {
      Resource
        .make {
          self.startAwaitExit
        } {
          _.cancel
        }
        .as(())
    }
  }

  implicit class ResourceOps[F[_], A](val self: Resource[F, A]) extends AnyVal {
    def withTimeoutRelease(
      duration: FiniteDuration
    )(implicit t: Temporal[F]): Resource[F, A] = {
      Resource(self.allocated.map { case (a, release) => (a, release.timeoutTo(duration, Applicative[F].unit)) })
    }
  }

}
