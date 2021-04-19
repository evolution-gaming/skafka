package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.Instant
import java.util.{Map => MapJ}

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.implicits._
import cats.{Monad, StackSafeMonad, ~>}
import com.evolutiongaming.catshelper.{MonadThrowable, ToTry}
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import com.evolutiongaming.skafka.consumer.DataModel._
import com.evolutiongaming.skafka.consumer.RebalanceCallback.RebalanceCallbackOps
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
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
  * new RebalanceListener1[IO] {
  *
  *  // one way is to import all methods, and then do `seek(...)`/`position(...)`/etc
  *  import RebalanceCallback._
  *
  *  // or assign it to a `val` and write code like `consumer.commit(offsets)`
  *  val consumer = RebalanceCallback
  *
  *  def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]) = {
  *    for {
  *      state <- lift(restoreStateFor(partitions))
  *      a     <- state.offsets.foldMapM(o => seek(o.partition, o.offset))
  *    } yield a
  *  }
  *
  *  def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]) =
  *    for {
  *      offsets <- lift(committableOffsetsFor(partitions))
  *      a       <- consumer.commit(offsets)
  *    } yield a
  *
  *  def onPartitionsLost(partitions: NonEmptySet[TopicPartition]) = empty
  * }
  * }}}
  * @see [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]]
  * @see [[RebalanceListener1]]
  */
sealed trait RebalanceCallback[+F[_], +A]

object RebalanceCallback extends RebalanceCallbackInstances with RebalanceCallbackApi[Nothing] {

  def api[F[_]]: RebalanceCallbackApi[F] = new RebalanceCallbackApi[F] {}

  private[consumer] def run[F[_]: ToTry, A](
    rebalanceCallback: RebalanceCallback[F, A],
    consumer: RebalanceConsumerJ
  ): Try[A] = rebalanceCallback.run(consumer)

  implicit class RebalanceCallbackOps[F[_], A](val self: RebalanceCallback[F, A]) extends AnyVal {

    def map[B](f: A => B): RebalanceCallback[F, B] = flatMap(a => pure(f(a)))

    def flatMap[B](f: A => RebalanceCallback[F, B]): RebalanceCallback[F, B] = Bind(self, f)

    def run(
      consumer: RebalanceConsumerJ
    )(implicit fToTry: ToTry[F]): Try[A] = self.mapK(ToTry.functionK).run2(consumer)

    def run2(
      consumer: RebalanceConsumerJ
    )(implicit appp: MonadThrowable[F]): F[A] = {
      type S = Any => RebalanceCallback[F, Any]

//      @tailrec
      def loop[A1](c: RebalanceCallback[F, A1], ss: List[S]): F[Any] = {
        c match {
          case c: Pure[A1] =>
            ss match {
              case Nil     => c.a.pure[F].widen
              case s :: ss => loop(s(c.a), ss)
            }
          case c: Lift[F, A1] =>
//            c.fa.toTry match {
//              case Success(a) =>
//                ss match {
//                  case Nil     => a.pure[Try]
//                  case s :: ss => loop(s(a), ss)
//                }
//              case Failure(a) => a.raiseError[Try, A1]
//            }
//            c.fa.widen

            c.fa.widen.flatMap { a =>
              ss match {
                case Nil     => a.pure[F].widen
                case s :: ss => loop(s(a), ss)
              }
            }
          case c: WithConsumer[A1] =>
            Try { c.f(consumer) } match {
              case Success(a) =>
                ss match {
                  case Nil     => a.pure[F].widen
                  case s :: ss => loop(s(a), ss)
                }
              case Failure(a) => a.raiseError[F, A1].widen
            }
          case c: Bind[F, _, A1] =>
            loop(c.source, c.fs.asInstanceOf[S] :: ss)
          case Error(a) =>
            a.raiseError[F, A1].widen
        }
      }

      for {
        a <- loop(self, List.empty)
      } yield a.asInstanceOf[A]
    }

    def mapK[G[_]](fg: F ~> G): RebalanceCallback[G, A] = {
      self match {
        case Pure(a)          => Pure(a)
        case Bind(source, f)  => Bind(source.mapK(fg), f andThen (_.mapK(fg)))
        case Lift(fa)         => Lift(fg(fa))
        case WithConsumer(f)  => WithConsumer(f)
        case Error(throwable) => Error(throwable)
      }
    }

  }

  implicit class RebalanceCallbackNothingOps[A](val self: RebalanceCallback[Nothing, A]) extends AnyVal {
    def effectAs[F[_]]: RebalanceCallback[F, A] = self
    def apply[F[_]]: RebalanceCallback[F, A]    = effectAs
  }

  object implicits {
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
    for {
      result <- WithConsumer(_.assignment())
      result <- fromTry(topicPartitionsSetF[Try](result))
    } yield result

  final def beginningOffsets(
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.beginningOffsets(partitions.asJava))

  final def beginningOffsets(
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.beginningOffsets(partitions.asJava, timeout.asJava))

  final def commit: RebalanceCallback[F, Unit] =
    WithConsumer(_.commitSync())

  final def commit(timeout: FiniteDuration): RebalanceCallback[F, Unit] =
    WithConsumer(_.commitSync(timeout.asJava))

  final def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]): RebalanceCallback[F, Unit] =
    WithConsumer(_.commitSync(asOffsetsAndMetadataJ(offsets)))

  final def commit(
    offsets: Nem[TopicPartition, OffsetAndMetadata],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Unit] =
    WithConsumer(_.commitSync(asOffsetsAndMetadataJ(offsets), timeout.asJava))

  final def committed(
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] =
    committed1(_.committed(partitions.asJava))

  final def committed(
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] =
    committed1(_.committed(partitions.asJava, timeout.asJava))

  final def endOffsets(
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.endOffsets(partitions.asJava))

  final def endOffsets(
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.endOffsets(partitions.asJava, timeout.asJava))

  final def groupMetadata: RebalanceCallback[F, ConsumerGroupMetadata] =
    WithConsumer(_.groupMetadata().asScala)

  final def topics: RebalanceCallback[F, Map[Topic, List[PartitionInfo]]] =
    for {
      result <- WithConsumer(_.listTopics())
      result <- fromTry(partitionsInfoMapF[Try](result))
    } yield result

  final def topics(timeout: FiniteDuration): RebalanceCallback[F, Map[Topic, List[PartitionInfo]]] =
    for {
      result <- WithConsumer(_.listTopics(timeout.asJava))
      result <- fromTry(partitionsInfoMapF[Try](result))
    } yield result

  final def offsetsForTimes(
    timestampsToSearch: Nem[TopicPartition, Instant]
  ): RebalanceCallback[F, Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    for {
      result <- WithConsumer(_.offsetsForTimes(timestampsToSearchJ(timestampsToSearch)))
      result <- fromTry(offsetsAndTimestampsMapF[Try](result))
    } yield result

  final def offsetsForTimes(
    timestampsToSearch: Nem[TopicPartition, Instant],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    for {
      result <- WithConsumer(_.offsetsForTimes(timestampsToSearchJ(timestampsToSearch), timeout.asJava))
      result <- fromTry(offsetsAndTimestampsMapF[Try](result))
    } yield result

  final def partitionsFor(topic: Topic): RebalanceCallback[F, List[PartitionInfo]] =
    for {
      result <- WithConsumer(_.partitionsFor(topic))
      result <- fromTry(partitionsInfoListF[Try](result))
    } yield result

  final def partitionsFor(
    topic: Topic,
    timeout: FiniteDuration
  ): RebalanceCallback[F, List[PartitionInfo]] =
    for {
      result <- WithConsumer(_.partitionsFor(topic, timeout.asJava))
      result <- fromTry(partitionsInfoListF[Try](result))
    } yield result

  final def paused: RebalanceCallback[F, Set[TopicPartition]] =
    for {
      result <- WithConsumer(_.paused())
      result <- fromTry(topicPartitionsSetF[Try](result))
    } yield result

  final def position(tp: TopicPartition): RebalanceCallback[F, Offset] =
    for {
      result <- WithConsumer(_.position(tp.asJava))
      result <- fromTry(Offset.of[Try](result))
    } yield result

  final def position(tp: TopicPartition, timeout: FiniteDuration): RebalanceCallback[F, Offset] =
    for {
      result <- WithConsumer(_.position(tp.asJava, timeout.asJava))
      result <- fromTry(Offset.of[Try](result))
    } yield result

  final def seek(partition: TopicPartition, offset: Offset): RebalanceCallback[F, Unit] =
    WithConsumer(_.seek(partition.asJava, offset.value))

  final def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): RebalanceCallback[F, Unit] =
    WithConsumer(_.seek(partition.asJava, offsetAndMetadata.asJava))

  final def seekToBeginning(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] =
    WithConsumer(_.seekToBeginning(partitions.asJava))

  final def seekToEnd(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] =
    WithConsumer(_.seekToEnd(partitions.asJava))

  final def subscription: RebalanceCallback[F, Set[Topic]] = WithConsumer(_.subscription().asScala.toSet)

  private def committed1(
    f: RebalanceConsumerJ => MapJ[TopicPartitionJ, OffsetAndMetadataJ]
  ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] = {
    for {
      result <- WithConsumer(f)
      result <- fromTry(committedOffsetsF[Try](result))
    } yield result
  }

  private def offsets1(
    f: RebalanceConsumerJ => MapJ[TopicPartitionJ, LongJ]
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] = {
    for {
      result <- WithConsumer(f)
      result <- fromTry(offsetsMapF[Try](result))
    } yield result
  }
}

abstract private[consumer] class RebalanceCallbackInstances {
  implicit val catsMonadForRebalanceCallbackWithNoEffect: Monad[RebalanceCallback[Nothing, *]] =
    catsMonadForRebalanceCallback[Nothing]

  implicit def catsMonadForRebalanceCallback[F[_]]: Monad[RebalanceCallback[F, *]] =
    new StackSafeMonad[RebalanceCallback[F, *]] {

      def pure[A](a: A): RebalanceCallback[F, A] =
        RebalanceCallback.pure(a)

      def flatMap[A, B](fa: RebalanceCallback[F, A])(f: A => RebalanceCallback[F, B]): RebalanceCallback[F, B] =
        new RebalanceCallbackOps(fa).flatMap(f)

    }
}

private[consumer] object DataModel {
  final case class Pure[+A](a: A) extends RebalanceCallback[Nothing, A]

  final case class Bind[F[_], S, +A](source: RebalanceCallback[F, S], fs: S => RebalanceCallback[F, A])
      extends RebalanceCallback[F, A]

  final case class Lift[F[_], A](fa: F[A]) extends RebalanceCallback[F, A]

  final case class WithConsumer[+A](f: RebalanceConsumerJ => A) extends RebalanceCallback[Nothing, A]

  final case class Error(throwable: Throwable) extends RebalanceCallback[Nothing, Nothing]

}
