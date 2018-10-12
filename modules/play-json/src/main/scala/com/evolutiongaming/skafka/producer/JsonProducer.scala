package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.{ToBytes, Topic}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.Future

trait JsonProducer[F[_]] {
  def apply(record: ProducerRecord[String, JsValue]): F[RecordMetadata]
}

object JsonProducer {

  val Empty: JsonProducer[Future] = apply(Producer.Send.Empty)

  implicit val JsValueToBytes: ToBytes[JsValue] = new ToBytes[JsValue] {
    def apply(value: JsValue, topic: Topic): Array[Byte] = Json.toBytes(value)
  }

  def apply[F[_]](send: Producer.Send[F])(): JsonProducer[F] = new JsonProducer[F] {
    def apply(record: ProducerRecord[String, JsValue]) = send(record)
  }
}