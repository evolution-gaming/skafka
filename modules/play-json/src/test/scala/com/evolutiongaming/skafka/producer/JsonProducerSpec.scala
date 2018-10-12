package com.evolutiongaming.skafka.producer

import java.nio.charset.StandardCharsets.UTF_8

import com.evolutiongaming.skafka.{Bytes, ToBytes, TopicPartition}
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{JsString, Json}

class JsonProducerSpec extends FunSuite with Matchers {

  test("apply") {
    val metadata = RecordMetadata(TopicPartition("topic", 0))
    var actual = Option.empty[(Option[Bytes], Option[Bytes])]
    type Id[A] = A
    val send = new Producer.Send[Id] {
      def apply[K, V](record: ProducerRecord[K, V])(implicit keyToBytes: ToBytes[K], valueToBytes: ToBytes[V]) = {
        val topic = record.topic
        val value = record.value.map(valueToBytes(_, topic))
        val key = record.key.map(keyToBytes(_, topic))
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
