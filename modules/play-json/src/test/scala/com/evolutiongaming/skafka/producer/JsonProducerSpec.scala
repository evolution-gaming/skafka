package com.evolutiongaming.skafka.producer

import java.nio.charset.StandardCharsets.UTF_8

import com.evolutiongaming.skafka.{Bytes, Partition, ToBytes, TopicPartition}
import play.api.libs.json.{JsString, Json}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.util.Try

class JsonProducerSpec extends AnyFunSuite with Matchers {
  test("apply") {
    val metadata = RecordMetadata(TopicPartition("topic", Partition.min))
    var actual = Option.empty[(Option[Bytes], Option[Bytes])]

    val send = new Producer.Send[Try] {
      def apply[K, V](
        record: ProducerRecord[K, V])(implicit
        toBytesK: ToBytes[Try, K],
        toBytesV: ToBytes[Try, V]
      ): Try[Try[RecordMetadata]] = {
        val topic = record.topic
        val value = record.value.flatMap(toBytesV(_, topic).toOption)
        val key = record.key.flatMap(toBytesK(_, topic).toOption)
        actual = Some((key, value))
        Try(Try(metadata))
      }
    }
    val producer = JsonProducer(send)

    val value = JsString("value")
    val key = "key"
    val record = ProducerRecord("topic", value, key)
    producer(record) shouldEqual Try(Try(metadata))
    val (Some(keyBytes), valueBytes) = actual.get
    new String(keyBytes, UTF_8) shouldEqual key
    valueBytes.map(Json.parse) shouldEqual Some(value)
  }
}
