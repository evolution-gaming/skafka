package com.evolutiongaming.skafka.producer

import com.evolutiongaming.nel.Nel

sealed trait Acks extends Product {
  def names: Nel[String]
}

object Acks {
  val Values: Set[Acks] = Set(All, None, One)

  case object All extends Acks {
    def names = Nel("all", "-1")
  }

  case object None extends Acks {
    def names = Nel("0")
  }

  case object One extends Acks {
    def names = Nel("1")
  }
}
