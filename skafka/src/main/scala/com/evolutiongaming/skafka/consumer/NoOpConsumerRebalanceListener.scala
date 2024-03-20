package com.evolutiongaming.skafka.consumer

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import java.util

/** Local version of NoOpConsumerRebalanceListener, because it was removed in kafka-clients 3.7.0
  */
private[skafka] final class NoOpConsumerRebalanceListener extends ConsumerRebalanceListener {

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = ()

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = ()
}
