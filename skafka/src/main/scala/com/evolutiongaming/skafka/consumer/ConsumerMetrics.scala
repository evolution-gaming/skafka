package com.evolutiongaming.skafka.consumer

import cats.Applicative
import com.evolutiongaming.skafka.{Topic, TopicPartition}
import cats.implicits._

trait ConsumerMetrics[F[_]] {

  def call(name: String, topic: Topic, latency: Long, success: Boolean): F[Unit]

  def poll(topic: Topic, bytes: Int, records: Int): F[Unit]

  def count(name: String, topic: Topic): F[Unit]

  def rebalance(name: String, topicPartition: TopicPartition): F[Unit]

  def listTopics(latency: Long): F[Unit]
}

object ConsumerMetrics {

  def empty[F[_] : Applicative]: ConsumerMetrics[F] = {
    
    val empty = ().pure[F]

    new ConsumerMetrics[F] {

      def call(name: String, topic: Topic, latency: Long, success: Boolean) = empty

      def poll(topic: Topic, bytes: Int, records: Int) = empty

      def count(name: String, topic: Topic) = empty

      def rebalance(name: String, topicPartition: TopicPartition) = empty

      def listTopics(latency: Long) = empty
    }
  }
}