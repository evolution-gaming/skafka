package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.{Bytes, ToBytes, Topic}
import play.api.libs.json.{JsValue, Writes}

object ToBytesFromWrites {

  def apply[A](implicit writes: Writes[A], toBytes: ToBytes[JsValue]): ToBytes[A] = new ToBytes[A] {
    def apply(value: A, topic: Topic): Bytes = {
      val json = writes.writes(value)
      toBytes(json, topic)
    }
  }
}
