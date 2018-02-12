package com.evolutiongaming.skafka.producer

import java.nio.charset.Charset

import com.evolutiongaming.skafka.Bytes

trait ToBytes[-T] {
  def apply(value: T): Bytes
}

object ToBytes {

  private val Empty: ToBytes[Any] = new ToBytes[Any] {
    def apply(value: Any) = Bytes.Empty
  }

  implicit val StringSerializer: ToBytes[String] = {
    val utf8 = Charset.forName("UTF-8")
    new ToBytes[String] {
      def apply(value: String): Bytes = value.getBytes(utf8)
    }
  }

  implicit val BytesSerializer: ToBytes[Bytes] = new ToBytes[Bytes] {
    def apply(value: Bytes): Bytes = value
  }

  def empty[T]: ToBytes[T] = Empty
}