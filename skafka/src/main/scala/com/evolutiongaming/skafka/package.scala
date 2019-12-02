package com.evolutiongaming

import cats.kernel.Monoid

package object skafka {

  type ClientId = String


  type Topic = String


  type Metadata = String

  object Metadata {
    val empty: Metadata = ""
  }


  type Bytes = Array[Byte]

  object Bytes {

    val empty: Bytes = Array.empty


    implicit val monoidBytes: Monoid[Bytes] = new Monoid[Bytes] {

      def empty = Bytes.empty

      def combine(x: Bytes, y: Bytes) = x ++ y
    }
  }
}