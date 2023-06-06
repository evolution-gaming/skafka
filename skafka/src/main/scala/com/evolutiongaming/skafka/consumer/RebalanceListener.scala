package com.evolutiongaming.skafka.consumer

import cats.data.{NonEmptySet => Nes}
import cats.implicits._
import cats.{Applicative, FlatMap, ~>}
import com.evolutiongaming.catshelper.{Log, MeasureDuration}
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.smetrics

/**
  * See [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener]]
  */
trait RebalanceListener[F[_]] {

  def onPartitionsAssigned(partitions: Nes[TopicPartition]): F[Unit]

  def onPartitionsRevoked(partitions: Nes[TopicPartition]): F[Unit]

  def onPartitionsLost(partitions: Nes[TopicPartition]): F[Unit]
}

object RebalanceListener {

  def empty[F[_]: Applicative]: RebalanceListener[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): RebalanceListener[F] = new RebalanceListener[F] {

    def onPartitionsAssigned(partitions: Nes[TopicPartition]) = unit

    def onPartitionsRevoked(partitions: Nes[TopicPartition]) = unit

    def onPartitionsLost(partitions: Nes[TopicPartition]) = unit
  }

  implicit class RebalanceListenerOps[F[_]](val self: RebalanceListener[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): RebalanceListener[G] = new RebalanceListener[G] {

      def onPartitionsAssigned(partitions: Nes[TopicPartition]) = {
        f(self.onPartitionsAssigned(partitions))
      }

      def onPartitionsRevoked(partitions: Nes[TopicPartition]) = {
        f(self.onPartitionsRevoked(partitions))
      }
      def onPartitionsLost(partitions: Nes[TopicPartition]) = {
        f(self.onPartitionsLost(partitions))
      }
    }

    @deprecated("use `withLogging1` instead", "15.2.0")
    def withLogging(
      log: Log[F]
    )(implicit F: FlatMap[F], measureDuration: smetrics.MeasureDuration[F]): RebalanceListener[F] = {
      implicit val md: MeasureDuration[F] = smetrics.MeasureDuration[F].toCatsHelper
      withLogging1(log)
    }

    def withLogging1(log: Log[F])(implicit F: FlatMap[F], measureDuration: MeasureDuration[F]): RebalanceListener[F] =
      new RebalanceListener[F] {

        def onPartitionsAssigned(partitions: Nes[TopicPartition]) = {
          for {
            d <- MeasureDuration[F].start
            a <- self.onPartitionsAssigned(partitions)
            d <- d
            _ <- log.debug(s"onPartitionsAssigned ${partitions.mkString_(", ")} in ${d.toMillis}ms")
          } yield a
        }

        def onPartitionsRevoked(partitions: Nes[TopicPartition]) = {
          for {
            d <- MeasureDuration[F].start
            a <- self.onPartitionsRevoked(partitions)
            d <- d
            _ <- log.debug(s"onPartitionsRevoked ${partitions.mkString_(", ")} in ${d.toMillis}ms")
          } yield a
        }

        def onPartitionsLost(partitions: Nes[TopicPartition]) = {
          for {
            d <- MeasureDuration[F].start
            a <- self.onPartitionsLost(partitions)
            d <- d
            _ <- log.debug(s"onPartitionsLost ${partitions.mkString_(", ")} in ${d.toMillis}ms")
          } yield a
        }
      }

  }
}
