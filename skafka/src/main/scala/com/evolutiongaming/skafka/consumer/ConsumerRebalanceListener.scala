package com.evolutiongaming.skafka.consumer

import cats.Applicative
import cats.syntax.all._
import cats.data.{NonEmptySet => Nes}
import com.evolutiongaming.skafka.TopicPartition

trait ConsumerRebalanceListener[F[_]] {

  def onPartitionsAssigned(
    partitions: Nes[TopicPartition],
    consumer: ListenerConsumer[F]
  ): F[ListenerConsumer[F]]

  def onPartitionsRevoked(partitions: Nes[TopicPartition],
                          consumer: ListenerConsumer[F]): F[ListenerConsumer[F]]

  def onPartitionsLost(partitions: Nes[TopicPartition],
                       consumer: ListenerConsumer[F]): F[ListenerConsumer[F]]

}

object ConsumerRebalanceListener {
  def noOp[F[_]: Applicative]: ConsumerRebalanceListener[F] =
    new ConsumerRebalanceListener[F] {
      def onPartitionsAssigned(
        partitions: Nes[TopicPartition],
        consumer: ListenerConsumer[F]
      ): F[ListenerConsumer[F]] = consumer.pure

      def onPartitionsRevoked(
        partitions: Nes[TopicPartition],
        consumer: ListenerConsumer[F]
      ): F[ListenerConsumer[F]] = consumer.pure

      def onPartitionsLost(
        partitions: Nes[TopicPartition],
        consumer: ListenerConsumer[F]
      ): F[ListenerConsumer[F]] = consumer.pure
    }
}
