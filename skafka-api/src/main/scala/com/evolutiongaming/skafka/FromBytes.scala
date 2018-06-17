package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8


trait FromBytes[T] {
  def apply(bytes: Bytes): T
}

object FromBytes {

  implicit val StringFromBytes: FromBytes[String] = new FromBytes[String] {
    def apply(bytes: Bytes) = new String(bytes, UTF_8)
  }

  implicit val BytesFromBytes: FromBytes[Bytes] = new FromBytes[Bytes] {
    def apply(value: Bytes): Bytes = value
  }

  def apply[T](value: T): FromBytes[T] = new FromBytes[T] {
    def apply(bytes: Bytes) = value
  }
}