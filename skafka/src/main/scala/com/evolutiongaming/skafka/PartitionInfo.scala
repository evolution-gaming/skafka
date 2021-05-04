package com.evolutiongaming.skafka

import org.apache.kafka.common.Node

final case class PartitionInfo(
  topicPartition: TopicPartition,
  leader: Node,
  replicas: List[Node]        = Nil,
  inSyncReplicas: List[Node]  = Nil,
  offlineReplicas: List[Node] = Nil
) {

  def topic: Topic = topicPartition.topic

  def partition: Partition = topicPartition.partition
}
