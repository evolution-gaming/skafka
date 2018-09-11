package com.evolutiongaming.skafka.producer

import java.nio.charset.StandardCharsets.UTF_8

import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.skafka.{Bytes, ToBytes, TopicPartition}
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{JsString, Json}

class JsonProducerSpec extends FunSuite with Matchers {

  test("apply") {
    val metadata = RecordMetadata(TopicPartition("topic", 0))
    var actual = Option.empty[(Option[Bytes], Option[Bytes])]
    val send = new Producer.Send {
      def apply[K, V](record: ProducerRecord[K, V])(implicit valueToBytes: ToBytes[V], keyToBytes: ToBytes[K]) = {
        val topic = record.topic
        val value = record.value.map(valueToBytes(_, topic))
        val key = record.key.map(keyToBytes(_, topic))
        actual = Some((key, value))
        metadata.future
      }
    }
    val producer = JsonProducer(send)

    val value = JsString("value")
    val key = "key"
    val record = ProducerRecord("topic", value, key)
    producer(record).value.get.get shouldEqual metadata
    val (Some(keyBytes), valueBytes) = actual.get
    new String(keyBytes, UTF_8) shouldEqual key
    valueBytes.map(Json.parse) shouldEqual Some(value)
  }
}
