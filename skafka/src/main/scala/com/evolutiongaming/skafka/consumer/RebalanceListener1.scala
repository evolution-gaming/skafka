package com.evolutiongaming.skafka.consumer

import cats.data.NonEmptySet
import cats.~>
import com.evolutiongaming.skafka.TopicPartition

/**
  * Will be converted to [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]] during consumer.subscribe
  *
  * Uses [[RebalanceCallback]] to describe the actions to be performed during rebalance.
  *
  * Please refer to [[RebalanceCallback]] for more details.
  *
  * Here is scala version (please note that it does not have all imports) for a callback implementation for saving offsets,
  * inspired by [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]] documentation.
  *
  * Compiling and working example is available here:
  *   TODO: replace with shorter github link after merge of https://github.com/evolution-gaming/skafka/pull/122
  * https://github.com/evolution-gaming/skafka/blob/d2af038b012523533f2b73d432721d6d1e7cebbe/skafka/src/test/scala/com/evolutiongaming/skafka/consumer/RebalanceListener1Spec.scala
  * {{{
  *
  * class SaveOffsetsOnRebalance[F[_]: Applicative] extends RebalanceListener1[F] {
  *  import RebalanceCallback._
  *
  *  def onPartitionsAssigned(partitions: Nes[TopicPartition]) =
  *    for {
  *      // read the offsets from an external store using some custom code not described here
  *      offsets <- lift(readOffsetsFromExternalStore[F](partitions))
  *      _       <- offsets.toList.traverse_ { case (partition, offset) => seek(partition, offset) }
  *    } yield ()
  *
  * def onPartitionsRevoked(partitions: Nes[TopicPartition]) =
  *    for {
  *      positions <- partitions.foldM(Map.empty[TopicPartition, Offset]) {
  *        case (offsets, partition) =>
  *          for {
  *            position <- position(partition)
  *          } yield offsets + (partition -> position)
  *      }
  *      // save the offsets in an external store using some custom code not described here
  *      _ <- lift(saveOffsetsInExternalStore[F](positions))
  *    } yield ()
  *
  *  // do not need to save the offsets since these partitions are probably owned by other consumers already
  *  def onPartitionsLost(partitions: Nes[TopicPartition]) = empty
  * }
  *
  * }}}
  * @see [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]]
  * @see [[com.evolutiongaming.skafka.consumer.RebalanceCallback]]
  */
trait RebalanceListener1[F[_]] { self =>

  def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit]

  def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit]

  def onPartitionsLost(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit]

}

object RebalanceListener1 {

  implicit class RebalanceListener1Ops[F[_]](val self: RebalanceListener1[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G): RebalanceListener1[G] = new RebalanceListener1[G] {

      def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[G, Unit] =
        self.onPartitionsAssigned(partitions).mapK(fg)

      def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[G, Unit] =
        self.onPartitionsRevoked(partitions).mapK(fg)

      def onPartitionsLost(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[G, Unit] =
        self.onPartitionsLost(partitions).mapK(fg)
    }

  }
}
