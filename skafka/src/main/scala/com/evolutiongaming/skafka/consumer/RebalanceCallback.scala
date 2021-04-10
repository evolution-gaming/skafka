package com.evolutiongaming.skafka.consumer

import java.lang.{Long => LongJ}
import java.time.Instant
import java.util.{Map => MapJ}

import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.implicits._
import cats.~>
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.ConsumerConverters._
import com.evolutiongaming.skafka.consumer.RebalanceCallback.Helpers._
import org.apache.kafka.clients.consumer.{Consumer => ConsumerJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

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
sealed trait RebalanceCallback[+F[_], +A]

object RebalanceCallback {

  /**
    * Will do nothing just like [[org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener]]
    */
  def noOp[F[_]]: RebalanceCallback[F, Unit] = pure(())

  def pure[F[_], A](a: A): RebalanceCallback[F, A] = Pure(a)

  def lift[F[_], A](fa: F[A]): RebalanceCallback[F, A] = LiftStep(fa)

  def fromTry[F[_], A](fa: Try[A]): RebalanceCallback[F, A] = fa match {
    case Success(value)     => pure(value)
    case Failure(exception) => Error(exception)
  }

  def assignment[F[_]]: RebalanceCallback[F, Set[TopicPartition]] =
    for {
      result <- WithConsumer(_.assignment())
      result <- fromTry(result.asScala.toList.traverse { _.asScala[Try] })
    } yield result.toSet

  def beginningOffsets[F[_]](
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.beginningOffsets(partitions.asJava))

  def beginningOffsets[F[_]](
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.beginningOffsets(partitions.asJava, timeout.asJava))

  def commit[F[_]]: RebalanceCallback[F, Unit] =
    WithConsumer(_.commitSync())

  def commit[F[_]](timeout: FiniteDuration): RebalanceCallback[F, Unit] =
    WithConsumer(_.commitSync(timeout.asJava))

  def commit[F[_]](offsets: Nem[TopicPartition, OffsetAndMetadata]): RebalanceCallback[F, Unit] =
    WithConsumer(_.commitSync(asOffsetsJ(offsets)))

  def commit[F[_]](
    offsets: Nem[TopicPartition, OffsetAndMetadata],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Unit] =
    WithConsumer(_.commitSync(asOffsetsJ(offsets), timeout.asJava))

  def committed[F[_]](
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] =
    committed1(partitions, none)

  def committed[F[_]](
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] =
    committed1(partitions, timeout.some)

  def endOffsets[F[_]](
    partitions: Nes[TopicPartition]
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.endOffsets(partitions.asJava))

  def endOffsets[F[_]](
    partitions: Nes[TopicPartition],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Offset]] =
    offsets1(_.endOffsets(partitions.asJava, timeout.asJava))

  def groupMetadata[F[_]]: RebalanceCallback[F, ConsumerGroupMetadata] =
    WithConsumer(_.groupMetadata().asScala)

  def topics[F[_]]: RebalanceCallback[F, Map[Topic, List[PartitionInfo]]] =
    for {
      result <- WithConsumer(_.listTopics())
      result <- fromTry(partitionsInfoMapF[Try](result))
    } yield result

  def topics[F[_]](timeout: FiniteDuration): RebalanceCallback[F, Map[Topic, List[PartitionInfo]]] =
    for {
      result <- WithConsumer(_.listTopics(timeout.asJava))
      result <- fromTry(partitionsInfoMapF[Try](result))
    } yield result

  def offsetsForTimes[F[_]](
    timestampsToSearch: Nem[TopicPartition, Instant]
  ): RebalanceCallback[F, Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    for {
      result <- WithConsumer(_.offsetsForTimes(timestampsToSearchJ(timestampsToSearch)))
      result <- fromTry(offsetsAndTimestampsMapF[Try](result))
    } yield result

  def offsetsForTimes[F[_]](
    timestampsToSearch: Nem[TopicPartition, Instant],
    timeout: FiniteDuration
  ): RebalanceCallback[F, Map[TopicPartition, Option[OffsetAndTimestamp]]] =
    for {
      result <- WithConsumer(_.offsetsForTimes(timestampsToSearchJ(timestampsToSearch), timeout.asJava))
      result <- fromTry(offsetsAndTimestampsMapF[Try](result))
    } yield result

  def partitionsFor[F[_]](topic: Topic): RebalanceCallback[F, List[PartitionInfo]] =
    for {
      result <- WithConsumer(_.partitionsFor(topic))
      result <- fromTry(partitionsInfoListF[Try](result))
    } yield result

  def partitionsFor[F[_]](
    topic: Topic,
    timeout: FiniteDuration
  ): RebalanceCallback[F, List[PartitionInfo]] =
    for {
      result <- WithConsumer(_.partitionsFor(topic, timeout.asJava))
      result <- fromTry(partitionsInfoListF[Try](result))
    } yield result

  def paused[F[_]]: RebalanceCallback[F, Set[TopicPartition]] =
    for {
      result <- WithConsumer(c => c.paused())
      result <- fromTry(result.asScala.toList.traverse { _.asScala[Try] })
    } yield result.toSet

  def position[F[_]](tp: TopicPartition): RebalanceCallback[F, Offset] =
    for {
      result <- WithConsumer(c => c.position(tp.asJava))
      result <- fromTry(Offset.of[Try](result))
    } yield result

  def position[F[_]](tp: TopicPartition, timeout: FiniteDuration): RebalanceCallback[F, Offset] =
    for {
      result <- WithConsumer(_.position(tp.asJava, timeout.asJava))
      result <- fromTry(Offset.of[Try](result))
    } yield result

  def seek[F[_]](partition: TopicPartition, offset: Offset): RebalanceCallback[F, Unit] =
    WithConsumer(_.seek(partition.asJava, offset.value))

  def seek[F[_]](partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): RebalanceCallback[F, Unit] =
    WithConsumer(_.seek(partition.asJava, offsetAndMetadata.asJava))

  def seekToBeginning[F[_]](partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] =
    WithConsumer(_.seekToBeginning(partitions.asJava))

  def seekToEnd[F[_]](partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] =
    WithConsumer(_.seekToEnd(partitions.asJava))

  def subscription[F[_]]: RebalanceCallback[F, Set[Topic]] = WithConsumer(_.subscription().asScala.toSet)

  private[consumer] def run[F[_]: ToTry, A](
    rebalanceCallback: RebalanceCallback[F, A],
    consumer: ConsumerJ[_, _]
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
        case c: LiftStep[F, A1] =>
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
        case c: FlatMapStep[F, _, A1] =>
          loop(c.parent, c.f.asInstanceOf[S] :: ss)
        case Error(a) =>
          a.raiseError[Try, A1]
      }
    }

    Try(loop(rebalanceCallback, List.empty).asInstanceOf[Try[A]]).flatten
  }

  private final case class Pure[A](a: A) extends RebalanceCallback[Nothing, A]

  private final case class FlatMapStep[F[_], A, B](f: A => RebalanceCallback[F, B], parent: RebalanceCallback[F, A])
      extends RebalanceCallback[F, B]

  private final case class LiftStep[F[_], A](fa: F[A]) extends RebalanceCallback[F, A]

  private final case class WithConsumer[A](f: ConsumerJ[_, _] => A) extends RebalanceCallback[Nothing, A]

  private final case class Error(throwable: Throwable) extends RebalanceCallback[Nothing, Nothing]

//  private final case class FlatMapError[F[_], A](
//    f: Throwable => RebalanceCallback[F, A],
//    parent: RebalanceCallback[F, A]
//  ) extends RebalanceCallback[F, A]

//  private final case class MapError[F[_], A](f: Throwable => A, parent: RebalanceCallback[F, A])
//      extends RebalanceCallback[F, A]

  implicit class ListenerConsumerOps[F[_], A](val self: RebalanceCallback[F, A]) extends AnyVal {

    def map[B](f: A => B): RebalanceCallback[F, B] = flatMap(a => pure(f(a)))

    def flatMap[B](f: A => RebalanceCallback[F, B]): RebalanceCallback[F, B] = FlatMapStep(f, self)

//    def handleError(f: Throwable => A): RebalanceCallback[F, A] = handleErrorWith(f.andThen(Pure(_)))

//    def raiseError(e: Throwable): RebalanceCallback[Nothing, Nothing] = Error(e)

//    def handleErrorWith(f: Throwable => RebalanceCallback[F, A]): RebalanceCallback[F, A] = FlatMapError(f, self)

    def mapK[G[_]](fg: F ~> G): RebalanceCallback[G, A] = {
      self match {
        case Pure(a)                => Pure(a)
        case FlatMapStep(f, parent) => FlatMapStep(f andThen (_.mapK(fg)), parent.mapK(fg))
        case LiftStep(fa)           => LiftStep(fg(fa))
        case WithConsumer(f)        => WithConsumer(f)
        case Error(throwable)       => Error(throwable)
//        case FlatMapError(f, parent) => FlatMapError(f andThen (_.mapK(fg)), parent.mapK(fg))
      }
    }

  }

  private[consumer] object Helpers {

    def committed1[F[_]](
      partitions: Nes[TopicPartition],
      timeout: Option[FiniteDuration]
    ): RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] = {
      for {
        result <- WithConsumer { c =>
          timeout match {
            case Some(value) => c.committed(partitions.asJava, value.asJava)
            case None        => c.committed(partitions.asJava)
          }
        }
        result <- fromTry(committedOffsetsF[Try](result))
      } yield result
    }

    def offsets1[F[_]](
      f: ConsumerJ[_, _] => MapJ[TopicPartitionJ, LongJ]
    ): RebalanceCallback[F, Map[TopicPartition, Offset]] = {
      for {
        result <- WithConsumer(f)
        result <- fromTry(offsetsMapF[Try](result))
      } yield result
    }

    def timestampsToSearchJ(nem: Nem[TopicPartition, Instant]): MapJ[TopicPartitionJ, LongJ] = {
      nem.toSortedMap.asJavaMap(_.asJava, _.toEpochMilli)
    }

  }

//  implicit def monadThrowableRebalanceCallback[F[_]]: MonadThrowable[RebalanceCallback[F, *]] =
//    new MonadThrowable[RebalanceCallback[F, *]] {
//      def raiseError[A](e: scala.Throwable): RebalanceCallback[F, A] = ???
//
//      def handleErrorWith[A](fa: RebalanceCallback[F, A])(
//        f: scala.Throwable => RebalanceCallback[F, A]
//      ): RebalanceCallback[F, A] = ???
//      def flatMap[A, B](fa: RebalanceCallback[F, A])(
//        f: A => RebalanceCallback[F, B]
//      ): RebalanceCallback[F, B] = ???
//
//      def tailRecM[A, B](a: A)(
//        f: A => RebalanceCallback[F, scala.Either[A, B]]
//      ): RebalanceCallback[F, B] = ???
//
//      def pure[A](x: A): RebalanceCallback[F, A] = ???
//    }

}
