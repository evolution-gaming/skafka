package com.evolutiongaming.skafka.producer

import cats.effect.Async
import com.evolutiongaming.safeakka.actor.ActorLog

trait Log[F[_]] {
  def debug(msg: String): F[Unit]

  def error(msg: String): F[Unit]

}
object Log {
  def apply[F[_] : Log]: Log[F] = implicitly[Log[F]]

  def apply[F[_] : Async](actorLog: ActorLog): Log[F] = new Log[F] {
    override def debug(msg: String): F[Unit] = Async[F].delay(actorLog.debug(msg))

    override def error(msg: String): F[Unit] = Async[F].delay(actorLog.error(msg))
  }
}
