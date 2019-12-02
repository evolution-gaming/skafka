package com.evolutiongaming.skafka.producer

import java.nio.charset.StandardCharsets.UTF_8

import com.evolutiongaming.skafka.{Bytes, Partition, ToBytes, TopicPartition}
import play.api.libs.json.{JsString, Json}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JsonProducerSpec extends AnyFunSuite with Matchers {

  test("apply") {
    val metadata = RecordMetadata(TopicPartition("topic", Partition.min))
    var actual = Option.empty[(Option[Bytes], Option[Bytes])]
    type Id[A] = A
    val send = new Producer.Send[Id] {
      def apply[K, V](
        record: ProducerRecord[K, V])(implicit
        toBytesK: ToBytes[Id, K],
        toBytesV: ToBytes[Id, V]
      ) = {
        val topic = record.topic
        val value = record.value.map(toBytesV(_, topic))
        val key = record.key.map(toBytesK(_, topic))
        actual = Some((key, value))
        metadata
      }
    }
    val producer = JsonProducer(send)

    val value = JsString("value")
    val key = "key"
    val record = ProducerRecord("topic", value, key)
    producer(record) shouldEqual metadata
    val (Some(keyBytes), valueBytes) = actual.get
    new String(keyBytes, UTF_8) shouldEqual key
    valueBytes.map(Json.parse) shouldEqual Some(value)
  }
}
