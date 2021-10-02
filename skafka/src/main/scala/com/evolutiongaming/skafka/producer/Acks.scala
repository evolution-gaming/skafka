package com.evolutiongaming.skafka.producer

import cats.data.{NonEmptyList => Nel}

sealed trait Acks extends Product {
  def names: Nel[String]
}

object Acks {
  val Values: Set[Acks] = Set(All, None, One)

  def all: Acks = All

  def none: Acks = None

  def one: Acks = One

  case object All extends Acks {
    def names = Nel.of("all", "-1")
  }

  case object None extends Acks {
    def names = Nel.of("0")
  }

  case object One extends Acks {
    def names = Nel.of("1")
  }
}
