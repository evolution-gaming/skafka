package com.evolutiongaming.skafka.producer

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.producer.ProducerMetrics.Latency

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

  def empty[F[_] : Applicative]: ProducerMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): ProducerMetrics[F] = new ProducerMetrics[F] {

    def initTransactions(latency: Long): F[Unit] = unit

    val beginTransaction: F[Unit] = unit

    def sendOffsetsToTransaction(latency: Long): F[Unit] = unit

    def commitTransaction(latency: Long): F[Unit] = unit

    def abortTransaction(latency: Long): F[Unit] = unit

    def block(topic: Topic, latency: Latency) = unit

    def send(topic: Topic, latency: Long, bytes: Int): F[Unit] = unit

    def failure(topic: Topic, latency: Long): F[Unit] = unit

    def partitions(topic: Topic, latency: Latency): F[Unit] = unit

    def flush(latency: Long): F[Unit] = unit
  }
}

