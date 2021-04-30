package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.Instant
import java.util.{Map => MapJ}

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.implicits._
import cats.{Monad, StackSafeMonad, ~>}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import com.evolutiongaming.skafka.consumer.RebalanceCallback.Helpers._
import com.evolutiongaming.skafka.consumer.RebalanceCallback.RebalanceCallbackOps
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

import scala.annotation.tailrec
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

object RebalanceCallback extends RebalanceCallbackInstances {

  val empty: RebalanceCallback[Nothing, Unit] = pure(())

  def pure[A](a: A): RebalanceCallback[Nothing, A] = Pure(a)

  def lift[F[_], A](fa: F[A]): RebalanceCallback[F, A] = Lift(fa)

  def fromTry[A](fa: Try[A]): RebalanceCallback[Nothing, A] = fa match {
    case Success(value)     => pure(value)
    case Failure(exception) => Error(exception)
  }

  def assignment: RebalanceCallback[Nothing, Set[TopicPartition]] =
    for {
      result <- WithConsumer(_.assignment())
      result <- fromTry(topicPartitionsSetF[Try](result))
    } yield result

  def beginningOffsets(
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[Nothing, Map[TopicPartition, Offset]] =
    offsets1(_.beginningOffsets(partitions.asJava))

  def beginningOffsets(
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[Nothing, Map[TopicPartition, Offset]] =
    offsets1(_.beginningOffsets(partitions.asJava, timeout.asJava))

  def commit: RebalanceCallback[Nothing, Unit] =
    WithConsumer(_.commitSync())

  def commit(timeout: FiniteDuration): RebalanceCallback[Nothing, Unit] =
    WithConsumer(_.commitSync(timeout.asJava))

  def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]): RebalanceCallback[Nothing, Unit] =
    WithConsumer(_.commitSync(asOffsetsAndMetadataJ(offsets)))

  def commit(
    offsets: Nem[TopicPartition, OffsetAndMetadata],
    timeout: FiniteDuration
  ): RebalanceCallback[Nothing, Unit] =
    WithConsumer(_.commitSync(asOffsetsAndMetadataJ(offsets), timeout.asJava))

  def committed(
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[Nothing, Map[TopicPartition, OffsetAndMetadata]] =
    committed1(_.committed(partitions.asJava))

  def committed(
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[Nothing, Map[TopicPartition, OffsetAndMetadata]] =
    committed1(_.committed(partitions.asJava, timeout.asJava))

  def endOffsets(
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[Nothing, Map[TopicPartition, Offset]] =
    offsets1(_.endOffsets(partitions.asJava))

  def endOffsets(
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[Nothing, Map[TopicPartition, Offset]] =
    offsets1(_.endOffsets(partitions.asJava, timeout.asJava))

  def groupMetadata: RebalanceCallback[Nothing, ConsumerGroupMetadata] =
    WithConsumer(_.groupMetadata().asScala)

  def topics: RebalanceCallback[Nothing, Map[Topic, List[PartitionInfo]]] =
    for {
      result <- WithConsumer(_.listTopics())
      result <- fromTry(partitionsInfoMapF[Try](result))
    } yield result

  def topics(timeout: FiniteDuration): RebalanceCallback[Nothing, Map[Topic, List[PartitionInfo]]] =
    for {
      result <- WithConsumer(_.listTopics(timeout.asJava))
      result <- fromTry(partitionsInfoMapF[Try](result))
    } yield result

  def offsetsForTimes(
    timestampsToSearch: Nem[TopicPartition, Instant]
  ): RebalanceCallback[Nothing, Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    for {
      result <- WithConsumer(_.offsetsForTimes(timestampsToSearchJ(timestampsToSearch)))
      result <- fromTry(offsetsAndTimestampsMapF[Try](result))
    } yield result

  def offsetsForTimes(
    timestampsToSearch: Nem[TopicPartition, Instant],
    timeout: FiniteDuration
  ): RebalanceCallback[Nothing, Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    for {
      result <- WithConsumer(_.offsetsForTimes(timestampsToSearchJ(timestampsToSearch), timeout.asJava))
      result <- fromTry(offsetsAndTimestampsMapF[Try](result))
    } yield result

  def partitionsFor(topic: Topic): RebalanceCallback[Nothing, List[PartitionInfo]] =
    for {
      result <- WithConsumer(_.partitionsFor(topic))
      result <- fromTry(partitionsInfoListF[Try](result))
    } yield result

  def partitionsFor(
    topic: Topic,
    timeout: FiniteDuration
  ): RebalanceCallback[Nothing, List[PartitionInfo]] =
    for {
      result <- WithConsumer(_.partitionsFor(topic, timeout.asJava))
      result <- fromTry(partitionsInfoListF[Try](result))
    } yield result

  def paused: RebalanceCallback[Nothing, Set[TopicPartition]] =
    for {
      result <- WithConsumer(_.paused())
      result <- fromTry(topicPartitionsSetF[Try](result))
    } yield result

  def position(tp: TopicPartition): RebalanceCallback[Nothing, Offset] =
    for {
      result <- WithConsumer(_.position(tp.asJava))
      result <- fromTry(Offset.of[Try](result))
    } yield result

  def position(tp: TopicPartition, timeout: FiniteDuration): RebalanceCallback[Nothing, Offset] =
    for {
      result <- WithConsumer(_.position(tp.asJava, timeout.asJava))
      result <- fromTry(Offset.of[Try](result))
    } yield result

  def seek(partition: TopicPartition, offset: Offset): RebalanceCallback[Nothing, Unit] =
    WithConsumer(_.seek(partition.asJava, offset.value))

  def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): RebalanceCallback[Nothing, Unit] =
    WithConsumer(_.seek(partition.asJava, offsetAndMetadata.asJava))

  def seekToBeginning(partitions: Nes[TopicPartition]): RebalanceCallback[Nothing, Unit] =
    WithConsumer(_.seekToBeginning(partitions.asJava))

  def seekToEnd(partitions: Nes[TopicPartition]): RebalanceCallback[Nothing, Unit] =
    WithConsumer(_.seekToEnd(partitions.asJava))

  def subscription: RebalanceCallback[Nothing, Set[Topic]] = WithConsumer(_.subscription().asScala.toSet)

  private[consumer] def run[F[_]: ToTry, A](
    rebalanceCallback: RebalanceCallback[F, A],
    consumer: RebalanceConsumerJ
  ): Try[A] = {
    type S = Any => RebalanceCallback[F, Any]

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
          Try { c.f(consumer) } match {
            case Success(a) =>
              ss match {
                case Nil     => a.pure[Try]
                case s :: ss => loop(s(a), ss)
              }
            case Failure(a) => a.raiseError[Try, A1]
          }
        case c: Bind[F, _, A1] =>
          loop(c.source, c.fs.asInstanceOf[S] :: ss)
        case Error(a) =>
          a.raiseError[Try, A1]
      }
    }

    for {
      a <- loop(rebalanceCallback, List.empty)
      a <- Try { a.asInstanceOf[A] }
    } yield a
  }

  private final case class Pure[+A](a: A) extends RebalanceCallback[Nothing, A]

  private final case class Bind[F[_], S, +A](source: RebalanceCallback[F, S], fs: S => RebalanceCallback[F, A])
      extends RebalanceCallback[F, A]

  private final case class Lift[F[_], A](fa: F[A]) extends RebalanceCallback[F, A]

  private final case class WithConsumer[+A](f: RebalanceConsumerJ => A) extends RebalanceCallback[Nothing, A]

  private final case class Error(throwable: Throwable) extends RebalanceCallback[Nothing, Nothing]

  implicit class RebalanceCallbackOps[F[_], A](val self: RebalanceCallback[F, A]) extends AnyVal {

    def map[B](f: A => B): RebalanceCallback[F, B] = flatMap(a => pure(f(a)))

    def flatMap[B](f: A => RebalanceCallback[F, B]): RebalanceCallback[F, B] = Bind(self, f)

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

  private[consumer] object Helpers {

    def committed1(
      f: RebalanceConsumerJ => MapJ[TopicPartitionJ, OffsetAndMetadataJ]
    ): RebalanceCallback[Nothing, Map[TopicPartition, OffsetAndMetadata]] = {
      for {
        result <- WithConsumer(f)
        result <- fromTry(committedOffsetsF[Try](result))
      } yield result
    }

    def offsets1(
      f: RebalanceConsumerJ => MapJ[TopicPartitionJ, LongJ]
    ): RebalanceCallback[Nothing, Map[TopicPartition, Offset]] = {
      for {
        result <- WithConsumer(f)
        result <- fromTry(offsetsMapF[Try](result))
      } yield result
    }

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
