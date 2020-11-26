package com.evolutiongaming.skafka.producer

import java.time.Instant

import cats.effect.Sync
import cats.syntax.all._
import com.evolutiongaming.skafka._

final case class ProducerRecord[+K, +V](
  topic: Topic,
  value: Option[V] = None,
  key: Option[K] = None,
  partition: Option[Partition] = None,
  timestamp: Option[Instant] = None,
  headers: List[Header] = Nil)

object ProducerRecord {

  def apply[K, V](topic: Topic, value: V, key: K): ProducerRecord[K, V] = {
    ProducerRecord(topic = topic, value = Some(value), key = Some(key))
  }


  implicit class ProducerRecordOps[K, V](val self: ProducerRecord[K, V]) extends AnyVal {

    def toBytes[F[_] : Sync](implicit
      toBytesK: ToBytes[F, K],
      toBytesV: ToBytes[F, V]
    ): F[ProducerRecord[Bytes, Bytes]] = {
      val topic = self.topic

      for {
        k <- self.key.traverse { toBytesK(_, topic) }
        v <- self.value.traverse { toBytesV(_, topic) }
      } yield {
        self.copy(value = v, key = k)
      }
    }
  }
}
