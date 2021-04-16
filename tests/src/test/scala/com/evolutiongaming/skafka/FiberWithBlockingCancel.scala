package com.evolutiongaming.skafka

import cats.implicits._
import cats.effect.syntax.all._
import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, Fiber, Resource, Timer}

import scala.concurrent.duration.FiniteDuration

object FiberWithBlockingCancel {
  implicit class FiberWithBlockingCancelOps[F[_], A](val self: F[A]) extends AnyVal {
    def startAwaitExit(implicit c: Concurrent[F]): F[Fiber[F, A]] = {
      for {
        deferred <- Deferred[F, Unit]
        fiber <- self.guarantee {
          deferred.complete(()).handleError { _ => () }
        }.start
      } yield {
        new Fiber[F, A] {
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
    def withTimeoutRelease(duration: FiniteDuration)(implicit c: Concurrent[F], t: Timer[F]): Resource[F, A] = {
      Resource(self.allocated.map { case (a, release) => (a, release.timeout(duration)) })
    }
  }

}
