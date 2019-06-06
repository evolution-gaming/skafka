package com.evolutiongaming.skafka.consumer

import cats.Applicative
import cats.implicits._
import com.evolutiongaming.skafka.{Topic, TopicPartition}

trait ConsumerMetrics[F[_]] {

  def call(name: String, topic: Topic, latency: Long, success: Boolean): F[Unit]

  def poll(topic: Topic, bytes: Int, records: Int): F[Unit]

  def count(name: String, topic: Topic): F[Unit]

  def rebalance(name: String, topicPartition: TopicPartition): F[Unit]

  def listTopics(latency: Long): F[Unit]
}

object ConsumerMetrics {

  def empty[F[_] : Applicative]: ConsumerMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): ConsumerMetrics[F] = new ConsumerMetrics[F] {

    def call(name: String, topic: Topic, latency: Long, success: Boolean) = unit

    def poll(topic: Topic, bytes: Int, records: Int) = unit

    def count(name: String, topic: Topic) = unit

    def rebalance(name: String, topicPartition: TopicPartition) = unit

    def listTopics(latency: Long) = unit
  }
}