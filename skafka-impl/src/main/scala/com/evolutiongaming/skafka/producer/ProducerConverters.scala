package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.producer.Producer._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConverters._

private[producer] object ProducerConverters {

  implicit class RecordOps[K, V](val self: Record[K, V]) extends AnyVal {

    def asJava: ProducerRecord[K, V] = {
      new ProducerRecord[K, V](
        self.topic,
        self.partition.fold[java.lang.Integer](null) { x => x },
        self.timestamp.fold[java.lang.Long](null) { x => x },
        self.key,
        self.value,
        self.headers.map { _.asJava }.asJava)
    }
  }
}