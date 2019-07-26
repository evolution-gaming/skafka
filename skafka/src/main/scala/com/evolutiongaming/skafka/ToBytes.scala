package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8

import cats.implicits._
import cats.{Applicative, Contravariant, ~>}
import com.evolutiongaming.catshelper.FromTry


trait ToBytes[F[_], -A] { self =>

  def apply(a: A, topic: Topic): F[Bytes]
}

object ToBytes {

  def apply[F[_], A](implicit F: ToBytes[F, A]): ToBytes[F, A] = F


  def const[F[_] : Applicative, A](bytes: Bytes): ToBytes[F, A] = (_: A, _: Topic) => bytes.pure[F]


  def empty[F[_] : Applicative, A]: ToBytes[F, A] = const(Bytes.empty)


  implicit def contravariantToBytes[F[_]]: Contravariant[ToBytes[F, ?]] = new Contravariant[ToBytes[F, ?]] {

    def contramap[A, B](fa: ToBytes[F, A])(f: B => A) = (a: B, topic: Topic) => fa(f(a), topic)
  }


  implicit def stringToBytes[F[_] : FromTry]: ToBytes[F, String] = {
    (a: String, _: Topic) => FromTry[F].unsafe { a.getBytes(UTF_8) }
  }


  implicit def bytesToBytes[F[_] : Applicative]: ToBytes[F, Bytes] = (a: Bytes, _: Topic) => a.pure[F]


  implicit class ToBytesOps[F[_], A](val self: ToBytes[F, A]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ToBytes[G, A] = (a: A, topic: Topic) => f(self(a, topic))
  }
}