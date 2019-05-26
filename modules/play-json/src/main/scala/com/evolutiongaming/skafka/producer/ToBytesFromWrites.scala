package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.{ToBytes, Topic}
import play.api.libs.json.{JsValue, Writes}

object ToBytesFromWrites {

  def apply[A](implicit writes: Writes[A], toBytes: ToBytes[JsValue]): ToBytes[A] = (value: A, topic: Topic) => {
    val json = writes.writes(value)
    toBytes(json, topic)
  }
}
