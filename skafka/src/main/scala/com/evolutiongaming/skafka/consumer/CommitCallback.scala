package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka.{OffsetAndMetadata, TopicPartition}

import scala.util.Try

trait CommitCallback {
  def apply(offsets: Try[Map[TopicPartition, OffsetAndMetadata]]): Unit
}

object CommitCallback {
  val Empty: CommitCallback = new CommitCallback {
    def apply(offsets: Try[Map[TopicPartition, OffsetAndMetadata]]) = {}
  }
}
