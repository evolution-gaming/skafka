package com.evolutiongaming.skafka.consumer


import com.evolutiongaming.skafka._

import scala.collection.immutable.Iterable


trait RebalanceListener {

  def onPartitionsAssigned(partitions: Iterable[TopicPartition]): Unit

  def onPartitionsRevoked(partitions: Iterable[TopicPartition]): Unit
}

object RebalanceListener {

  val Empty: RebalanceListener = new RebalanceListener {
    def onPartitionsAssigned(partitions: Iterable[TopicPartition]) = {}
    def onPartitionsRevoked(partitions: Iterable[TopicPartition]) = {}
  }
}
