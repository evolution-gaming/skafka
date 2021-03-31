package com.evolutiongaming.skafka.consumer

import cats.data.{NonEmptySet => Nes}
import com.evolutiongaming.skafka.TopicPartition

/**
  * Will be converted to [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]] during consumer.subscribe
  * // TODO add most common usages in scaladoc from spec (using position/seek/commit)
  * // TODO rename to something more meaningful vs just appending `1` to existing name `RebalanceListener`
  * see [[com.evolutiongaming.skafka.consumer.RebalanceCallback]]
  */
trait RebalanceListener1[F[_]] {

  def onPartitionsAssigned(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit]

  def onPartitionsRevoked(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit]

  def onPartitionsLost(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit]

}

object RebalanceListener1 {
  def noOp[F[_]]: RebalanceListener1[F] =
    new RebalanceListener1[F] {

      def onPartitionsAssigned(
        partitions: Nes[TopicPartition]
      ): RebalanceCallback[F, Unit] = RebalanceCallback.noOp

      def onPartitionsRevoked(
        partitions: Nes[TopicPartition]
      ): RebalanceCallback[F, Unit] = RebalanceCallback.noOp

      def onPartitionsLost(
        partitions: Nes[TopicPartition]
      ): RebalanceCallback[F, Unit] = RebalanceCallback.noOp

    }

  // TODO uncomment and fix compilation
//  import cats.~>
//  implicit class RebalanceListener1Ops[F[_]](val self: RebalanceListener1[F]) extends AnyVal {
//    def mapK[G[_]](f: F ~> G): RebalanceListener1[G] = new RebalanceListener1[G] {
//
//      def onPartitionsAssigned(partitions: Nes[TopicPartition]) = {
//        f(self.onPartitionsAssigned(partitions))
//      }
//
//      def onPartitionsRevoked(partitions: Nes[TopicPartition]) = {
//        f(self.onPartitionsRevoked(partitions))
//      }
//      def onPartitionsLost(partitions: Nes[TopicPartition]) = {
//        f(self.onPartitionsLost(partitions))
//      }
//    }
//
//  }
}
