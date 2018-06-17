package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8

trait ToBytes[-T] {
  def apply(value: T, topic: Topic): Bytes
}

object ToBytes {

  private val Empty: ToBytes[Any] = new ToBytes[Any] {
    def apply(value: Any, topic: Topic) = Bytes.Empty
  }

  implicit val StringToBytes: ToBytes[String] = {
    new ToBytes[String] {
      def apply(value: String, topic: Topic): Bytes = value.getBytes(UTF_8)
    }
  }

  implicit val BytesToBytes: ToBytes[Bytes] = new ToBytes[Bytes] {
    def apply(value: Bytes, topic: Topic): Bytes = value
  }

  def empty[T]: ToBytes[T] = Empty
}