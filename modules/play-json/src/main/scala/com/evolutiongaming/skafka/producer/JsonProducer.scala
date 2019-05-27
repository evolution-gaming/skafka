package com.evolutiongaming.skafka.producer

import cats.Applicative
import com.evolutiongaming.skafka.{ToBytes, Topic}
import play.api.libs.json.{JsValue, Json}

trait JsonProducer[F[_]] {
  def apply(record: ProducerRecord[String, JsValue]): F[RecordMetadata]
}

object JsonProducer {

  def empty[F[_] : Applicative]: JsonProducer[F] = apply(Producer.Send.Empty[F])

  implicit val JsValueToBytes: ToBytes[JsValue] = (value: JsValue, _: Topic) => Json.toBytes(value)

  def apply[F[_]](send: Producer.Send[F])(): JsonProducer[F] = (record: ProducerRecord[String, JsValue]) => send(record)
}