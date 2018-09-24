package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8

trait ToBytes[-A] { self =>

  def apply(a: A, topic: Topic): Bytes

  final def imap[B](f: B => A): ToBytes[B] = new ToBytes[B] {
    def apply(value: B, topic: Topic) = self(f(value), topic)
  }
}

object ToBytes {

  private val Empty = const(Bytes.Empty)

  implicit val StringToBytes: ToBytes[String] = new ToBytes[String] {
    def apply(a: String, topic: Topic): Bytes = a.getBytes(UTF_8)
  }

  implicit val BytesToBytes: ToBytes[Bytes] = new ToBytes[Bytes] {
    def apply(value: Bytes, topic: Topic): Bytes = value
  }

  def empty[A]: ToBytes[A] = Empty

  def const[A](bytes: Bytes): ToBytes[A] = new ToBytes[A] {
    def apply(a: A, topic: Topic) = bytes
  }

  def apply[A](implicit toBytes: ToBytes[A]): ToBytes[A] = toBytes
}