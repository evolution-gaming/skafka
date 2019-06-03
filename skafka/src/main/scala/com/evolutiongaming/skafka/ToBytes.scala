package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8

trait ToBytes[-A] {
  self =>

  def apply(a: A, topic: Topic): Bytes

  final def imap[B](f: B => A): ToBytes[B] = (value: B, topic: Topic) => self(f(value), topic)
}

object ToBytes {

  private val _empty = const(Bytes.empty)

  implicit val StringToBytes: ToBytes[String] = (a: String, _: Topic) => a.getBytes(UTF_8)

  implicit val BytesToBytes: ToBytes[Bytes] = (value: Bytes, _: Topic) => value

  def empty[A]: ToBytes[A] = _empty

  def const[A](bytes: Bytes): ToBytes[A] = (_: A, _: Topic) => bytes

  def apply[A](implicit toBytes: ToBytes[A]): ToBytes[A] = toBytes
}