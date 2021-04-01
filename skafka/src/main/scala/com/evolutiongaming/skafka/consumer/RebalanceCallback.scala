package com.evolutiongaming.skafka.consumer

import cats.{Monad, ~>}
import cats.data.NonEmptyMap
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, TopicPartition}
import org.apache.kafka.clients.consumer.{Consumer => ConsumerJ}
import com.evolutiongaming.skafka.Converters._

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

// TODO review if we can do it without F[_] everywhere
// TODO add most common usages in scaladoc from spec (using position/seek/commit)
/*
def onPartitionAssigned(callback: RebalanceCallback[Unit]): ListenerConsumer[Unit] = {
  for {
    position <- consumer.position
    saved <- ListenerConsumer.lift { db.save(…) }
    result <- {
        if (saved) consumer.commit(…)
        else ListenerConsumer.unit
      }
  } yield result
}
 */
/**
  * Used to describe computations in callback methods of [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]]
  */
sealed trait RebalanceCallback[F[_], A] {
  def mapK[G[_]](fg: F ~> G): RebalanceCallback[G, A]
}

object RebalanceCallback {

  /**
    * Will do nothing just like [[org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener]]
    */
  def noOp[F[_]]: RebalanceCallback[F, Unit] = Pure(())

  def lift[F[_], A](fa: F[A]): RebalanceCallback[F, A] = LiftStep(fa)

  def commit[F[_]](offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): RebalanceCallback[F, Unit] =
    ConsumerStep(
      _.commitSync(
        offsets
          .toSortedMap
          .asJavaMap(_.asJava, _.asJava)
      )
    )

  def commit[F[_]](
    offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Unit] =
    ConsumerStep(
      _.commitSync(
        offsets
          .toSortedMap
          .asJavaMap(_.asJava, _.asJava),
        timeout.asJava
      )
    )

  // TODO using Offset.unsafe to avoid requiring ApplicativeThrowable as in Offset.of
  //  also it looks like consumer.position cannot return invalid offset
  def position[F[_]](tp: TopicPartition): RebalanceCallback[F, Offset] =
    ConsumerStep(c => Offset.unsafe(c.position(tp.asJava)))

  def position[F[_]](tp: TopicPartition, timeout: FiniteDuration): RebalanceCallback[F, Offset] =
    ConsumerStep(c => Offset.unsafe(c.position(tp.asJava, timeout.asJava)))

  private[consumer] def run[F[_]: Monad: ToTry, A](
    rc: RebalanceCallback[F, A],
    consumer: ConsumerJ[_, _]
  ): Try[A] = {

    // TODO @tailrec failed to convert map/flatMap to patmat in 5 minutes, was getting scalac errors about type mismatches
    def loop[A1](rc: RebalanceCallback[F, A1]): Try[A1] = {
      rc match {
        case Pure(a)                => Try(a)
        case MapStep(f, parent)     => loop(parent).map(f)
        case FlatMapStep(f, parent) => loop(parent).flatMap { a => loop(f(a)) }
        case LiftStep(fa)           => fa.toTry
        case ConsumerStep(f)        => Try { f(consumer) }
      }
    }

    loop(rc)
  }

  private final case class Pure[F[_], A](a: A) extends RebalanceCallback[F, A] {
    def mapK[G[_]](fg: F ~> G): RebalanceCallback[G, A] = Pure[G, A](a)
  }

  private final case class MapStep[F[_], A, B](f: A => B, parent: RebalanceCallback[F, A])
      extends RebalanceCallback[F, B] {
    def mapK[G[_]](fg: F ~> G): RebalanceCallback[G, B] = MapStep[G, A, B](f, parent.mapK(fg))
  }

  private final case class FlatMapStep[F[_], A, B](f: A => RebalanceCallback[F, B], parent: RebalanceCallback[F, A])
      extends RebalanceCallback[F, B] {
    def mapK[G[_]](fg: F ~> G): RebalanceCallback[G, B] = FlatMapStep[G, A, B](f andThen (_.mapK(fg)), parent.mapK(fg))
  }

  private final case class LiftStep[F[_], A](fa: F[A]) extends RebalanceCallback[F, A] {
    def mapK[G[_]](fg: F ~> G): RebalanceCallback[G, A] = LiftStep[G, A](fg(fa))
  }

  private final case class ConsumerStep[F[_], A](f: ConsumerJ[_, _] => A) extends RebalanceCallback[F, A] {
    def mapK[G[_]](fg: F ~> G): RebalanceCallback[G, A] = ConsumerStep[G, A](f)
  }

  implicit class ListenerConsumerOps[F[_], A](val self: RebalanceCallback[F, A]) extends AnyVal {

    def map[B](f: A => B): RebalanceCallback[F, B] = MapStep(f, self)

    def flatMap[B](f: A => RebalanceCallback[F, B]): RebalanceCallback[F, B] = FlatMapStep(f, self)

  }

}
