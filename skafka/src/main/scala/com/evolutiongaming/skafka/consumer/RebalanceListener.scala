package com.evolutiongaming.skafka.consumer


import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import cats.{Applicative, FlatMap, ~>}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.smetrics.MeasureDuration


/**
  * See [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]]
  */
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


    def withLogging(
      log: Log[F])(implicit
      F: FlatMap[F],
      measureDuration: MeasureDuration[F]
    ): RebalanceListener[F] = new RebalanceListener[F] {

      def onPartitionsAssigned(partitions: Nel[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- self.onPartitionsAssigned(partitions)
          d <- d
          _ <- log.debug(s"onPartitionsAssigned ${ partitions.mkString_(", ") } in ${ d.toMillis }ms")
        } yield a
      }

      def onPartitionsRevoked(partitions: Nel[TopicPartition]) = {
        for {
          d <- MeasureDuration[F].start
          a <- self.onPartitionsRevoked(partitions)
          d <- d
          _ <- log.debug(s"onPartitionsRevoked ${ partitions.mkString_(", ") } in ${ d.toMillis }ms")
        } yield a
      }
    }
  }
}
