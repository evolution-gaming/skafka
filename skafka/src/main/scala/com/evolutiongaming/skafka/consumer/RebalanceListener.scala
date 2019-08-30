package com.evolutiongaming.skafka.consumer


import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.skafka.TopicPartition


trait RebalanceListener[F[_]] {

  def onPartitionsAssigned(partitions: Nel[TopicPartition]): F[Unit]

  def onPartitionsRevoked(partitions: Nel[TopicPartition]): F[Unit]
}

object RebalanceListener {

  def empty[F[_] : Applicative]: RebalanceListener[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): RebalanceListener[F] = new RebalanceListener[F] {

    def onPartitionsAssigned(partitions: Nel[TopicPartition]) = unit

    def onPartitionsRevoked(partitions: Nel[TopicPartition]) = unit
  }

  
  implicit class RebalanceListenerOps[F[_]](val self: RebalanceListener[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): RebalanceListener[G] = new RebalanceListener[G] {

      def onPartitionsAssigned(partitions: Nel[TopicPartition]) = {
        f(self.onPartitionsAssigned(partitions))
      }

      def onPartitionsRevoked(partitions: Nel[TopicPartition]) = {
        f(self.onPartitionsRevoked(partitions))
      }
    }
  }
}
