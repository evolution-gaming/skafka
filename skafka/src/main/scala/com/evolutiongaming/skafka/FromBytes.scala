package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8

trait FromBytes[A] { self =>
  
  def apply(bytes: Bytes, topic: Topic): A

  final def map[B](f: A => B): FromBytes[B] = new FromBytes[B] {
    def apply(bytes: Bytes, topic: Topic) = f(self(bytes, topic))
  }
}

object FromBytes {

  implicit val StringFromBytes: FromBytes[String] = new FromBytes[String] {
    def apply(bytes: Bytes, topic: Topic) = new String(bytes, UTF_8)
  }

  implicit val BytesFromBytes: FromBytes[Bytes] = new FromBytes[Bytes] {
    def apply(bytes: Bytes, topic: Topic): Bytes = bytes
  }

  def apply[A](implicit fromBytes: FromBytes[A]): FromBytes[A] = fromBytes

  def const[A](a: A): FromBytes[A] = new FromBytes[A] {
    def apply(bytes: Bytes, topic: Topic) = a
  }
}