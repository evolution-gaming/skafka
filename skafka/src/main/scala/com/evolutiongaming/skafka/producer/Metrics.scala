package com.evolutiongaming.skafka.producer

import cats.Applicative
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.producer.Metrics.Latency

trait Metrics[F[_]] {

  def initTransactions(latency: Long): F[Unit]

  val beginTransaction: F[Unit]

  def sendOffsetsToTransaction(latency: Long): F[Unit]

  def commitTransaction(latency: Long): F[Unit]

  def abortTransaction(latency: Long): F[Unit]

  def send(topic: Topic, latency: Long, bytes: Int): F[Unit]

  def failure(topic: Topic, latency: Long): F[Unit]

  def partitions(topic: Topic, latency: Latency): F[Unit]

  def flush(latency: Long): F[Unit]

  def close(latency: Long): F[Unit]
}

object Metrics {

  type Latency = Long

  def empty[F[_] : Applicative]: Metrics[F] = new Metrics[F] {
    val empty: F[Unit] = Applicative[F].pure(())

    override def initTransactions(latency: Long): F[Unit] = empty

    override val beginTransaction: F[Unit] = empty

    override def sendOffsetsToTransaction(latency: Long): F[Unit] = empty

    override def commitTransaction(latency: Long): F[Unit] = empty

    override def abortTransaction(latency: Long): F[Unit] = empty

    override def send(topic: Topic, latency: Long, bytes: Int): F[Unit] = empty

    override def failure(topic: Topic, latency: Long): F[Unit] = empty

    override def partitions(topic: Topic, latency: Latency): F[Unit] = empty

    override def flush(latency: Long): F[Unit] = empty

    override def close(latency: Long): F[Unit] = empty
  }
}

