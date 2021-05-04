package com.evolutiongaming.skafka

import java.nio.charset.StandardCharsets.UTF_8

import cats.implicits._
import cats.{Applicative, Functor, ~>}
import com.evolutiongaming.catshelper.FromTry

trait FromBytes[F[_], A] {

  def apply(bytes: Bytes, topic: Topic): F[A]
}

object FromBytes {

  def apply[F[_], A](implicit F: FromBytes[F, A]): FromBytes[F, A] = F

  def const[F[_]: Applicative, A](a: A): FromBytes[F, A] = (_: Bytes, _: Topic) => a.pure[F]

  implicit def functorFromBytes[F[_]: Functor]: Functor[FromBytes[F, ?]] = new Functor[FromBytes[F, ?]] {

    def map[A, B](fa: FromBytes[F, A])(f: A => B) = new FromBytes[F, B] {

      def apply(bytes: Bytes, topic: Topic) = fa(bytes, topic).map(f)
    }
  }

  implicit def stringFromBytes[F[_]: FromTry]: FromBytes[F, String] = { (a: Bytes, _: Topic) =>
    FromTry[F].unsafe { new String(a, UTF_8) }
  }

  implicit def bytesFromBytes[F[_]: Applicative]: FromBytes[F, Bytes] = (a: Bytes, _: Topic) => a.pure[F]

  implicit class FromBytesOps[F[_], A](val self: FromBytes[F, A]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): FromBytes[G, A] = (a: Bytes, topic: Topic) => f(self(a, topic))
  }
}
