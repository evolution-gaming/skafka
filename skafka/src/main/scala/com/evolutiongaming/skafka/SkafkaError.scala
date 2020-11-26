package com.evolutiongaming.skafka

import cats.syntax.all._

import scala.util.control.NoStackTrace

case class SkafkaError(
  msg: String,
  cause: Option[Throwable] = None
) extends RuntimeException(msg, cause.orNull) with NoStackTrace

object SkafkaError {

  def apply(msg: String, cause: Throwable): SkafkaError = SkafkaError(msg, cause.some)
}