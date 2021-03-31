package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, TopicPartition}
import cats.data.NonEmptyMap

trait ListenerConsumer[F[_]] {

  def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): F[Unit]

  def position(partition: TopicPartition): F[Offset]

}
