package com.evolutiongaming.skafka.producer

import com.evolutiongaming.skafka.{Bytes, ToBytes, Topic}
import play.api.libs.json.{JsValue, Writes}

object ToBytesFromWrites {

  def apply[T](implicit writes: Writes[T], toBytes: ToBytes[JsValue]): ToBytes[T] = new ToBytes[T] {
    def apply(value: T, topic: Topic): Bytes = {
      val json = writes.writes(value)
      toBytes(json, topic)
    }
  }
}
