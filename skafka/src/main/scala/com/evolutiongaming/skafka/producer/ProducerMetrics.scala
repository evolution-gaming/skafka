package com.evolutiongaming.skafka.producer

import cats.Applicative
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.producer.ProducerMetrics.Latency
import cats.implicits._

trait ProducerMetrics[F[_]] {

  def initTransactions(latency: Long): F[Unit]

  def beginTransaction: F[Unit]

  def sendOffsetsToTransaction(latency: Long): F[Unit]

  def commitTransaction(latency: Long): F[Unit]

  def abortTransaction(latency: Long): F[Unit]

  def send(topic: Topic, latency: Long, bytes: Int): F[Unit]

  def block(topic: Topic, latency: Long): F[Unit]

  def failure(topic: Topic, latency: Long): F[Unit]

  def partitions(topic: Topic, latency: Latency): F[Unit]

  def flush(latency: Long): F[Unit]
}

object ProducerMetrics {

  type Latency = Long

  def empty[F[_] : Applicative]: ProducerMetrics[F] = {

    val empty = ().pure[F]

    new ProducerMetrics[F] {

      def initTransactions(latency: Long): F[Unit] = empty

      val beginTransaction: F[Unit] = empty

      def sendOffsetsToTransaction(latency: Long): F[Unit] = empty

      def commitTransaction(latency: Long): F[Unit] = empty

      def abortTransaction(latency: Long): F[Unit] = empty

      def block(topic: Topic, latency: Latency) = empty

      def send(topic: Topic, latency: Long, bytes: Int): F[Unit] = empty

      def failure(topic: Topic, latency: Long): F[Unit] = empty

      def partitions(topic: Topic, latency: Latency): F[Unit] = empty

      def flush(latency: Long): F[Unit] = empty
    }
  }
}

