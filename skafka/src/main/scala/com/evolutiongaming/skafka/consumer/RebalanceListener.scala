package com.evolutiongaming.skafka.consumer


import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.skafka.TopicPartition

import scala.collection.immutable.Iterable


trait RebalanceListener[F[_]] {

  def onPartitionsAssigned(partitions: Iterable[TopicPartition]): F[Unit]

  def onPartitionsRevoked(partitions: Iterable[TopicPartition]): F[Unit]
}

object RebalanceListener {

  def empty[F[_] : Applicative]: RebalanceListener[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): RebalanceListener[F] = new RebalanceListener[F] {

    def onPartitionsAssigned(partitions: Iterable[TopicPartition]) = unit

    def onPartitionsRevoked(partitions: Iterable[TopicPartition]) = unit
  }

  
  implicit class RebalanceListenerOps[F[_]](val self: RebalanceListener[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): RebalanceListener[G] = new RebalanceListener[G] {

      def onPartitionsAssigned(partitions: Iterable[TopicPartition]) = {
        f(self.onPartitionsAssigned(partitions))
      }

      def onPartitionsRevoked(partitions: Iterable[TopicPartition]) = {
        f(self.onPartitionsRevoked(partitions))
      }
    }
  }
}
