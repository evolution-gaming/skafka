package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.{ToBytes, Topic}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.Future

trait JsonProducer {
  def apply(record: ProducerRecord[String, JsValue]): Future[RecordMetadata]
}

object JsonProducer {

  val Empty: JsonProducer = apply(Producer.Send.Empty)

  implicit val JsValueToBytes: ToBytes[JsValue] = new ToBytes[JsValue] {
    def apply(value: JsValue, topic: Topic) = Json.toBytes(value)
  }

  def apply(send: Producer.Send)(): JsonProducer = new JsonProducer {
    def apply(record: ProducerRecord[String, JsValue]) = send(record)
  }
}