package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.Instant
import java.util.{Map => MapJ}

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.implicits._
import cats.{Monad, ~>}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{ApplicativeThrowable, MonadThrowable, ToTry}
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import com.evolutiongaming.skafka.consumer.RebalanceCallback.Helpers._
import org.apache.kafka.clients.consumer.{Consumer => ConsumerJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

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
  * Allows to describe computations in callback methods of [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]]
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

  def assignment[F[_]]: RebalanceCallback[F, Set[TopicPartition]] =
    ConsumerStep(
      _.assignment()
        .asScala
        .toSet
        .map(unsafeTopicPartition)
    )

  def beginningOffsets[F[_]: MonadThrowable](
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.beginningOffsets(partitions.asJava))

  def beginningOffsets[F[_]: MonadThrowable](
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.beginningOffsets(partitions.asJava, timeout.asJava))

  def commit[F[_]]: RebalanceCallback[F, Unit] =
    ConsumerStep(_.commitSync())

  def commit[F[_]](timeout: FiniteDuration): RebalanceCallback[F, Unit] =
    ConsumerStep(_.commitSync(timeout.asJava))

  def commit[F[_]](offsets: Nem[TopicPartition, OffsetAndMetadata]): RebalanceCallback[F, Unit] =
    ConsumerStep(_.commitSync(asOffsetsJ(offsets)))

  def commit[F[_]](
    offsets: Nem[TopicPartition, OffsetAndMetadata],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Unit] =
    ConsumerStep(_.commitSync(asOffsetsJ(offsets), timeout.asJava))

  def committed[F[_]: MonadThrowable](
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] =
    committed1(partitions, none)

  def committed[F[_]: MonadThrowable](
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] =
    committed1(partitions, timeout.some)

  def endOffsets[F[_]: MonadThrowable](
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.endOffsets(partitions.asJava))

  def endOffsets[F[_]: MonadThrowable](
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.endOffsets(partitions.asJava, timeout.asJava))

  def groupMetadata[F[_]]: RebalanceCallback[F, ConsumerGroupMetadata] =
    ConsumerStep(_.groupMetadata().asScala)

  def topics[F[_]: MonadThrowable]: RebalanceCallback[F, Map[Topic, List[PartitionInfo]]] =
    for {
      result <- ConsumerStep(_.listTopics())
      result <- lift(partitionsInfoMapF[F](result))
    } yield result

  def topics[F[_]: MonadThrowable](timeout: FiniteDuration): RebalanceCallback[F, Map[Topic, List[PartitionInfo]]] =
    for {
      result <- ConsumerStep(_.listTopics(timeout.asJava))
      result <- lift(partitionsInfoMapF[F](result))
    } yield result

  def offsetsForTimes[F[_]: MonadThrowable](
    timestampsToSearch: Nem[TopicPartition, Instant]
  ): RebalanceCallback[F, Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    for {
      result <- ConsumerStep(_.offsetsForTimes(timestampsToSearchJ(timestampsToSearch)))
      result <- lift(offsetsAndTimestampsMapF[F](result))
    } yield result

  def offsetsForTimes[F[_]: MonadThrowable](
    timestampsToSearch: Nem[TopicPartition, Instant],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    for {
      result <- ConsumerStep(_.offsetsForTimes(timestampsToSearchJ(timestampsToSearch), timeout.asJava))
      result <- lift(offsetsAndTimestampsMapF[F](result))
    } yield result

  def partitionsFor[F[_]: ApplicativeThrowable](topic: Topic): RebalanceCallback[F, List[PartitionInfo]] =
    for {
      result <- ConsumerStep(_.partitionsFor(topic))
      result <- lift(partitionsInfoListF[F](result))
    } yield result

  def partitionsFor[F[_]: ApplicativeThrowable](
    topic: Topic,
    timeout: FiniteDuration
  ): RebalanceCallback[F, List[PartitionInfo]] =
    for {
      result <- ConsumerStep(_.partitionsFor(topic, timeout.asJava))
      result <- lift(partitionsInfoListF[F](result))
    } yield result

  def paused[F[_]]: RebalanceCallback[F, Set[TopicPartition]] =
    ConsumerStep(
      _.paused().asScala.toSet.map(unsafeTopicPartition)
    )

  // TODO using Offset.unsafe to avoid requiring ApplicativeThrowable as in Offset.of
  //  also it looks like consumer.position cannot return invalid offset
  def position[F[_]](tp: TopicPartition): RebalanceCallback[F, Offset] =
    ConsumerStep(c => Offset.unsafe(c.position(tp.asJava)))

  def position[F[_]](tp: TopicPartition, timeout: FiniteDuration): RebalanceCallback[F, Offset] =
    ConsumerStep(c => Offset.unsafe(c.position(tp.asJava, timeout.asJava)))

  def seek[F[_]](partition: TopicPartition, offset: Offset): RebalanceCallback[F, Unit] =
    ConsumerStep(c => c.seek(partition.asJava, offset.value))

  def seek[F[_]](partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): RebalanceCallback[F, Unit] =
    ConsumerStep(c => c.seek(partition.asJava, offsetAndMetadata.asJava))

  def seekToBeginning[F[_]](partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] =
    ConsumerStep(c => c.seekToBeginning(partitions.asJava))

  def seekToEnd[F[_]](partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] =
    ConsumerStep(c => c.seekToEnd(partitions.asJava))

  def subscription[F[_]]: RebalanceCallback[F, Set[Topic]] = ConsumerStep(_.subscription().asScala.toSet)

  private[consumer] def run[F[_]: Monad: ToTry, A](
    rc: RebalanceCallback[F, A],
    consumer: ConsumerJ[_, _]
  ): Try[Any] = {
    type Value = Try[Any]
    type S     = Try[Any] => Try[RebalanceCallback[F, _]]
    case class Cont(cb: RebalanceCallback[F, _], tail: List[S])

    @tailrec
    def loop[A1](rc: RebalanceCallback[F, A1], lst: List[S]): Try[Any] = {
      def fold(tr: Try[Any]): Either[Cont, Value] = {
        lst match {
          case Nil => Right(tr)
          case head :: tail =>
            head(tr) match {
              case Success(value) => Left(Cont(value, tail))
              case e              => Right(e)
            }
        }
      }
      rc match {
        case Pure(a) =>
          fold(Try(a)) match {
            case Left(c)  => loop(c.cb, c.tail)
            case Right(t) => t
          }
        case LiftStep(fa) =>
          fold(fa.toTry) match {
            case Left(c)  => loop(c.cb, c.tail)
            case Right(t) => t
          }
        case ConsumerStep(f) =>
          fold(Try { f(consumer) }) match {
            case Left(c)  => loop(c.cb, c.tail)
            case Right(t) => t
          }
        case MapStep(f, parent) =>
          val mapTry: S = (tr: Try[Any]) => tr.map(a => Pure(f(a)))
          loop(parent, mapTry :: lst)
        case FlatMapStep(f, parent) =>
          val flatMapTry = (tr: Try[Any]) => tr.map(a => f(a))
          loop(parent, flatMapTry :: lst)
      }
    }
    loop(rc, List.empty)
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

  private[consumer] object Helpers {

    def committed1[F[_]: MonadThrowable](
      partitions: Nes[TopicPartition],
      timeout: Option[FiniteDuration]
    ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] = {
      for {
        result <- ConsumerStep { c =>
          timeout match {
            case Some(value) => c.committed(partitions.asJava, value.asJava)
            case None        => c.committed(partitions.asJava)
          }
        }
        result <- lift(committedOffsetsF[F](result))
      } yield result
    }

    def offsets1[F[_]: MonadThrowable](
      f: ConsumerJ[_, _] => MapJ[TopicPartitionJ, LongJ]
    ): RebalanceCallback[F, Map[TopicPartition, Offset]] = {
      for {
        result <- ConsumerStep(f)
        result <- lift(offsetsMapF[F](result))
      } yield result
    }

    def unsafeTopicPartition(tp: TopicPartitionJ): TopicPartition =
      TopicPartition(tp.topic(), Partition.unsafe(tp.partition()))

    def timestampsToSearchJ(nem: Nem[TopicPartition, Instant]): MapJ[TopicPartitionJ, LongJ] = {
      nem.toSortedMap.asJavaMap(_.asJava, _.toEpochMilli)
    }

  }

}
