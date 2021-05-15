package com.evolutiongaming.skafka.consumer

import java.time.Instant

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.implicits._
import cats.{Monad, StackSafeMonad, ~>}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{MonadThrowable, ToTry}
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.DataModel._
import com.evolutiongaming.skafka.consumer.RebalanceCallback.RebalanceCallbackOps

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/**
  * Describes computations in callback methods of [[RebalanceListener1]].
  *
  * `RebalanceCallback` is just a data structure (a description of things to be done),
  * so calling `RebalanceCallback.seek(...)` for example does not execute the `seek` right away.
  *
  * However all consumer related methods are executed on a `consumer.poll(...)` thread,
  * at the time of interpretation/execution of the `RebalanceCallback` data structure.
  *
  * The computations' result is awaited on a `poll` thread,
  * just as it would with blocking java API of `ConsumerRebalanceListener`.
  *
  * Errors from consumer related methods are thrown in a `poll` thread,
  * and currently there's no way to provide recovering code for [[RebalanceCallback]],
  * but it's planned to be [[https://github.com/evolution-gaming/skafka/issues/128 added]].
  *
  * Unhandled errors from lifted computations are thrown in a `poll` thread,
  * currently it's only possible to handle those errors from within lifted F[_] context.
  *
  * Usage:
  * {{{
  * new RebalanceListener1WithConsumer[IO] {
  *
  *  // import is needed to use `fa.lift` syntax where
  *  // `fa: F[A]`
  *  // `fa.lift: RebalanceCallback[F, A]`
  *  import RebalanceCallback.syntax._
  *
  *  def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]) = {
  *    for {
  *      state <- restoreStateFor(partitions).lift
  *      a     <- state.offsets.foldMapM(o => consumer.seek(o.partition, o.offset))
  *    } yield a
  *  }
  *
  *  def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]) =
  *    for {
  *      offsets <- committableOffsetsFor(partitions).lift
  *      a       <- consumer.commit(offsets)
  *    } yield a
  *
  *  def onPartitionsLost(partitions: NonEmptySet[TopicPartition]) = RebalanceCallback.empty
  * }
  * }}}
  * @see [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]]
  * @see [[RebalanceListener1]]
  */
sealed trait RebalanceCallback[+F[_], +A]

object RebalanceCallback extends RebalanceCallbackInstances with RebalanceCallbackApi[Nothing] {

  def api[F[_]]: RebalanceCallbackApi[F] = new RebalanceCallbackApi[F] {}

  implicit class RebalanceCallbackOps[F[_], A](val self: RebalanceCallback[F, A]) extends AnyVal {

    def map[B](f: A => B): RebalanceCallback[F, B] = flatMap(a => pure(f(a)))

    def flatMap[B](f: A => RebalanceCallback[F, B]): RebalanceCallback[F, B] = Bind(() => self, f)

    def handleErrorWith(f: Throwable => RebalanceCallback[F, A]): RebalanceCallback[F, A] =
      HandleErrorWith(() => self, f)

    def run(
      consumer: RebalanceConsumer
    )(implicit fToTry: ToTry[F]): Try[A] = {
      type S = Any => RebalanceCallback[F, Any]

      def continue[A1](c: RebalanceCallback[F, A1], ss: List[S]): Try[Any] =
        loop(c, ss)

      @tailrec
      def loop[A1](c: RebalanceCallback[F, A1], ss: List[S]): Try[Any] = {
        c match {
          case c: Pure[A1] =>
            ss match {
              case Nil     => c.a.pure[Try]
              case s :: ss => loop(s(c.a), ss)
            }
          case c: Lift[F, A1] =>
            c.fa.toTry match {
              case Success(a) =>
                ss match {
                  case Nil     => a.pure[Try]
                  case s :: ss => loop(s(a), ss)
                }
              case Failure(a) => a.raiseError[Try, A1]
            }
          case c: WithConsumer[A1] =>
            c.f(consumer) match {
              case Success(a) =>
                ss match {
                  case Nil     => a.pure[Try]
                  case s :: ss => loop(s(a), ss)
                }
              case Failure(a) => a.raiseError[Try, A1]
            }
          case c: Bind[F, _, A1] =>
            loop(c.source(), c.fs.asInstanceOf[S] :: ss)
          case Error(a) =>
            a.raiseError[Try, A1]
          case HandleErrorWith(source, fe) =>
            continue(source(), Nil) match {
              case Failure(e) =>
                loop(fe(e), ss)
              case Success(a) =>
                ss match {
                  case Nil     => a.pure[Try]
                  case s :: ss => loop(s(a), ss)
                }
            }
        }
      }

      for {
        a <- loop(self, List.empty)
        a <- Try { a.asInstanceOf[A] }
      } yield a
    }

    def toF(
      consumer: RebalanceConsumer
    )(implicit MT: MonadThrowable[F]): F[A] = {
      type S = Any => RebalanceCallback[F, Any]

      def continue[A1](c: RebalanceCallback[F, A1], ss: List[S]): F[Any] =
        loop(c, ss)

      @tailrec
      def loop[A1](c: RebalanceCallback[F, A1], ss: List[S]): F[Any] = {
        c match {
          case c: Pure[A1] =>
            ss match {
              case Nil     => c.a.pure[F].widen
              case s :: ss => loop(s(c.a), ss)
            }
          case c: Lift[F, A1] =>
            c.fa.widen.flatMap { a =>
              ss match {
                case Nil     => a.pure[F].widen
                case s :: ss => continue(s(a), ss)
              }
            }
          case c: WithConsumer[A1] =>
            // a trick to make `c.f` lazy if F[_] is lazy
            // by moving it into `flatMap`
            // using this trick as it does not require Defer/Sync instances
            val fa = for {
              _ <- ().pure[F]
              a <- c.f(consumer).fold(_.raiseError[F, A1], _.pure[F])
            } yield a
            loop(Lift(fa), ss)
          case c: Bind[F, _, A1] =>
            loop(c.source(), c.fs.asInstanceOf[S] :: ss)
          case Error(a) =>
            a.raiseError[F, A1].widen
          case HandleErrorWith(source, fe) =>
            continue(source(), Nil).attempt.flatMap {
              case Left(e) =>
                continue(fe(e), ss)
              case Right(a) =>
                ss match {
                  case Nil     => a.pure[F]
                  case s :: ss => continue(s(a), ss)
                }
            }
        }
      }

      for {
        a <- loop(self, List.empty)
      } yield a.asInstanceOf[A]
    }

    def mapK[G[_]](fg: F ~> G): RebalanceCallback[G, A] = {
      self match {
        case Pure(a)                     => Pure(a)
        case Bind(source, f)             => Bind(() => source().mapK(fg), f andThen (_.mapK(fg)))
        case Lift(fa)                    => Lift(fg(fa))
        case WithConsumer(f)             => WithConsumer(f)
        case Error(throwable)            => Error(throwable)
        case HandleErrorWith(source, fe) => HandleErrorWith(() => source().mapK(fg), fe andThen (_.mapK(fg)))
      }
    }

  }

  implicit class RebalanceCallbackNothingOps[A](val self: RebalanceCallback[Nothing, A]) extends AnyVal {
    def effectAs[F[_]]: RebalanceCallback[F, A] = self
    def apply[F[_]]: RebalanceCallback[F, A]    = effectAs
  }

  object syntax {
    implicit class RebalanceCallbackSyntaxOps[F[_], A](val self: F[A]) extends AnyVal {
      def lift: RebalanceCallback[F, A] = RebalanceCallback.lift(self)
    }
  }

}

sealed trait RebalanceCallbackApi[F[_]] {

  final val empty: RebalanceCallback[F, Unit] = pure(())

  final def pure[A](a: A): RebalanceCallback[F, A] = Pure(a)

  final def lift[F1[_], A](fa: F1[A]): RebalanceCallback[F1, A] = Lift(fa)

  final def fromTry[A](fa: Try[A]): RebalanceCallback[F, A] = fa match {
    case Success(value)     => pure(value)
    case Failure(exception) => Error(exception)
  }

  final def assignment: RebalanceCallback[F, Set[TopicPartition]] =
    WithConsumer(_.assignment())

  final def beginningOffsets(
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    WithConsumer(_.beginningOffsets(partitions))

  final def beginningOffsets(
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    WithConsumer(_.beginningOffsets(partitions, timeout))

  final def commit: RebalanceCallback[F, Unit] =
    WithConsumer(_.commit())

  final def commit(timeout: FiniteDuration): RebalanceCallback[F, Unit] =
    WithConsumer(_.commit(timeout))

  final def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]): RebalanceCallback[F, Unit] =
    WithConsumer(_.commit(offsets))

  final def commit(
    offsets: Nem[TopicPartition, OffsetAndMetadata],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Unit] =
    WithConsumer(_.commit(offsets, timeout))

  final def committed(
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] =
    WithConsumer(_.committed(partitions))

  final def committed(
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] =
    WithConsumer(_.committed(partitions, timeout))

  final def endOffsets(
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    WithConsumer(_.endOffsets(partitions))

  final def endOffsets(
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    WithConsumer(_.endOffsets(partitions, timeout))

  final def groupMetadata: RebalanceCallback[F, ConsumerGroupMetadata] =
    WithConsumer(_.groupMetadata())

  final def topics: RebalanceCallback[F, Map[Topic, List[PartitionInfo]]] =
    WithConsumer(_.topics())

  final def topics(timeout: FiniteDuration): RebalanceCallback[F, Map[Topic, List[PartitionInfo]]] =
    WithConsumer(_.topics(timeout))

  final def offsetsForTimes(
    timestampsToSearch: Nem[TopicPartition, Instant]
  ): RebalanceCallback[F, Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    WithConsumer(_.offsetsForTimes(timestampsToSearch))

  final def offsetsForTimes(
    timestampsToSearch: Nem[TopicPartition, Instant],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    WithConsumer(_.offsetsForTimes(timestampsToSearch, timeout))

  final def partitionsFor(topic: Topic): RebalanceCallback[F, List[PartitionInfo]] =
    WithConsumer(_.partitionsFor(topic))

  final def partitionsFor(
    topic: Topic,
    timeout: FiniteDuration
  ): RebalanceCallback[F, List[PartitionInfo]] =
    WithConsumer(_.partitionsFor(topic, timeout))

  final def paused: RebalanceCallback[F, Set[TopicPartition]] =
    WithConsumer(_.paused())

  final def position(partition: TopicPartition): RebalanceCallback[F, Offset] =
    WithConsumer(_.position(partition))

  final def position(partition: TopicPartition, timeout: FiniteDuration): RebalanceCallback[F, Offset] =
    WithConsumer(_.position(partition, timeout))

  final def seek(partition: TopicPartition, offset: Offset): RebalanceCallback[F, Unit] =
    WithConsumer(_.seek(partition, offset))

  final def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): RebalanceCallback[F, Unit] =
    WithConsumer(_.seek(partition, offsetAndMetadata))

  final def seekToBeginning(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] =
    WithConsumer(_.seekToBeginning(partitions))

  final def seekToEnd(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] =
    WithConsumer(_.seekToEnd(partitions))

  final def subscription: RebalanceCallback[F, Set[Topic]] =
    WithConsumer(_.subscription())

}

private[consumer] trait RebalanceCallbackLowPrioInstances {

  implicit val catsMonadForRebalanceCallbackWithNoEffect: Monad[RebalanceCallback[Nothing, *]] =
    new MonadForRebalanceCallback[Nothing]

  implicit def catsMonadForRebalanceCallback[F[_]]: Monad[RebalanceCallback[F, *]] =
    new MonadForRebalanceCallback[F]

  class MonadForRebalanceCallback[F[_]] extends StackSafeMonad[RebalanceCallback[F, *]] {
    override def flatMap[A, B](fa: RebalanceCallback[F, A])(f: A => RebalanceCallback[F, B]): RebalanceCallback[F, B] =
      new RebalanceCallbackOps(fa).flatMap(f)
    override def pure[A](a: A): RebalanceCallback[F, A] =
      RebalanceCallback.pure(a)
  }

}

abstract private[consumer] class RebalanceCallbackInstances extends RebalanceCallbackLowPrioInstances {

  implicit def catsMonadThrowableForRebalanceCallback[F[_]: MonadThrowable]: MonadThrowable[RebalanceCallback[F, *]] =
    new MonadThrowableForRebalanceCallback[F]

  private class MonadThrowableForRebalanceCallback[F[_]]
      extends MonadForRebalanceCallback[F]
      with MonadThrowable[RebalanceCallback[F, *]] {

    override def raiseError[A](e: Throwable): RebalanceCallback[F, A] =
      RebalanceCallback.fromTry(Failure(e))

    override def handleErrorWith[A](
      cb: RebalanceCallback[F, A]
    )(f: Throwable => RebalanceCallback[F, A]): RebalanceCallback[F, A] =
      cb.handleErrorWith(f)
  }

}

private[consumer] object DataModel {
  final case class Pure[+A](a: A) extends RebalanceCallback[Nothing, A]

  // lazy source is needed to avoid StackOverflowError in mapK implementation
  final case class Bind[F[_], S, +A](source: () => RebalanceCallback[F, S], fs: S => RebalanceCallback[F, A])
      extends RebalanceCallback[F, A]

  final case class Lift[F[_], A](fa: F[A]) extends RebalanceCallback[F, A]

  final case class WithConsumer[+A](f: RebalanceConsumer => Try[A]) extends RebalanceCallback[Nothing, A]

  final case class Error(throwable: Throwable) extends RebalanceCallback[Nothing, Nothing]

  final case class HandleErrorWith[F[_], A](
    source: () => RebalanceCallback[F, A],
    fe: Throwable => RebalanceCallback[F, A]
  ) extends RebalanceCallback[F, A]

}
