package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8

trait FromBytes[T] {
  def apply(bytes: Bytes, topic: Topic): T
}

object FromBytes {

  implicit val StringFromBytes: FromBytes[String] = new FromBytes[String] {
    def apply(bytes: Bytes, topic: Topic) = new String(bytes, UTF_8)
  }

  implicit val BytesFromBytes: FromBytes[Bytes] = new FromBytes[Bytes] {
    def apply(value: Bytes, topic: Topic): Bytes = value
  }

  def apply[T](value: T): FromBytes[T] = new FromBytes[T] {
    def apply(bytes: Bytes, topic: Topic) = value
  }
}