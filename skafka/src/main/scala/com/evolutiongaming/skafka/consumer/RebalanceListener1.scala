package com.evolutiongaming.skafka.consumer

import cats.data.{NonEmptySet => Nes}
import cats.~>
import com.evolutiongaming.skafka.TopicPartition

/**
  * Will be converted to [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]] during consumer.subscribe
  *
  * Uses [[RebalanceCallback]] to describe the actions to be performed during rebalance.
  *
  * Please refer to [[RebalanceCallback]] for more details.
  *
  * Below is an example inspired by [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]] documentation.
  *
  * Compiling and working example is available [[https://github.com/evolution-gaming/skafka/blob/master/skafka/src/test/scala/com/evolutiongaming/skafka/consumer/RebalanceListener1Spec.scala here]]
  * {{{
  *
  * class SaveOffsetsOnRebalance[F[_]: Applicative] extends RebalanceListener1[F] {
  *
  *   // one way is to import all methods, and then do `seek(...)`/`position(...)`/etc
  *   import RebalanceCallback._
  *
  *   // or assign it to a `val` and write code like `consumer.position(partition)`
  *   val consumer = RebalanceCallback
  *
  *  def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]) =
  *    for {
  *      // read the offsets from an external store using some custom code not described here
  *      offsets <- lift(readOffsetsFromExternalStore[F](partitions))
  *      a       <- offsets.toList.foldMapM { case (partition, offset) => seek(partition, offset) }
  *    } yield a
  *
  * def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]) =
  *    for {
  *      positions <- partitions.foldM(Map.empty[TopicPartition, Offset]) {
  *        case (offsets, partition) =>
  *          for {
  *            position <- consumer.position(partition)
  *          } yield offsets + (partition -> position)
  *      }
  *      // save the offsets in an external store using some custom code not described here
  *      a <- lift(saveOffsetsInExternalStore[F](positions))
  *    } yield a
  *
  *  // do not need to save the offsets since these partitions are probably owned by other consumers already
  *  def onPartitionsLost(partitions: NonEmptySet[TopicPartition]) = empty
  * }
  *
  * }}}
  * @see [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]]
  * @see [[com.evolutiongaming.skafka.consumer.RebalanceCallback]]
  */
trait RebalanceListener1[F[_]] {

  def onPartitionsAssigned(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit]

  def onPartitionsRevoked(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit]

  def onPartitionsLost(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit]

}

object RebalanceListener1 {

  def empty[F[_]]: RebalanceListener1[F] = const(RebalanceCallback.pure(()))

  def const[F[_]](unit: RebalanceCallback[F, Unit]): RebalanceListener1[F] = new RebalanceListener1[F] {

    def onPartitionsAssigned(partitions: Nes[TopicPartition]) = unit

    def onPartitionsRevoked(partitions: Nes[TopicPartition]) = unit

    def onPartitionsLost(partitions: Nes[TopicPartition]) = unit
  }

  implicit class RebalanceListener1Ops[F[_]](val self: RebalanceListener1[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G): RebalanceListener1[G] = new RebalanceListener1[G] {

      def onPartitionsAssigned(partitions: Nes[TopicPartition]): RebalanceCallback[G, Unit] =
        self.onPartitionsAssigned(partitions).mapK(fg)

      def onPartitionsRevoked(partitions: Nes[TopicPartition]): RebalanceCallback[G, Unit] =
        self.onPartitionsRevoked(partitions).mapK(fg)

      def onPartitionsLost(partitions: Nes[TopicPartition]): RebalanceCallback[G, Unit] =
        self.onPartitionsLost(partitions).mapK(fg)
    }

  }
}
