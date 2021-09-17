package com.evolutiongaming.skafka.consumer

import cats.Applicative
import cats.effect.Concurrent
import cats.effect.concurrent.{Deferred, Ref, Semaphore}
import cats.effect.implicits._
import cats.implicits._

/** This rather complex mechanism exists for sake of ensuring that "RebalanceListener" executed within scope of
  * "Consumer.poll" asynchronously. however providing same order semantic as in native consumer, like:
  *
  *   1. poll enters 2. rebalanceListener 3. poll exits
  *
  * Calls order in details as following:
  *
  *   1. Consumer.poll enters 2. ConsumerNative.poll enters 3. RebalanceListenerNative enters 4. Deferred created for
  *      partitionsAssigned & partitionsRevoked and stored in Ref 5. RebalanceListener launched, potentially
  *      asynchronously on different threads 6. RebalanceListenerNative exits, while RebalanceListener still running 7.
  *      ConsumerNative.poll exits 8. Consumer.poll cannot exit until Deferred is completed 9. RebalanceListener
  *      finishes and completes deferred 10. Consumer.poll exits as Deferred is completed
  *
  * This mechanism also allows calling Consumer withing RebalanceListener, with on exception: call of Consumer.poll will
  * lead to deadlock
  */
trait SerialListeners[F[_]] {

  def around[A](fa: F[A]): F[F[A]]

  def listener[A](fa: F[A]): F[F[A]]
}

object SerialListeners {

  def empty[F[_]: Applicative]: SerialListeners[F] = new SerialListeners[F] {

    def around[A](fa: F[A]) = fa.pure[F]

    def listener[A](fa: F[A]) = fa.pure[F]
  }

  def of[F[_]: Concurrent]: F[SerialListeners[F]] = {
    val empty = ().pure[F]
    for {
      ref       <- Ref[F].of(empty)
      semaphore <- Semaphore[F](1)
    } yield {
      new SerialListeners[F] {

        def around[A](fa: F[A]) = {
          val result = for {
            _ <- ref.set(empty)
            a <- fa.attempt
            l <- ref.modify { a => (empty, a) }
          } yield for {
            _ <- l
            a <- a.liftTo[F]
          } yield a
          result.uncancelable
        }

        def listener[A](fa: F[A]) = {
          val result = for {
            d <- Deferred[F, Either[Throwable, Unit]]
            l <- ref.modify { a => (d.get.rethrow, a) }
            f = for {
              a <- l.productR(semaphore.withPermit(fa)).attempt
              _ <- d.complete(a.void)
              a <- a.liftTo[F]
            } yield a
            f <- f.start
          } yield {
            f.join
          }
          result.uncancelable
        }
      }
    }
  }
}
