package com.evolutiongaming.skafka

import org.apache.kafka.common.Node

final case class PartitionInfo(
  topicPartition: TopicPartition,
  leader: Node,
  replicas: List[Node],
  inSyncReplicas: List[Node],
  offlineReplicas: List[Node]) {

  def topic: Topic = topicPartition.topic

  def partition: Partition = topicPartition.partition
}
