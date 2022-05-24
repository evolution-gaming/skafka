package com.evolutiongaming.skafka

final case class ClientMetric[F[_]](
  name: String,
  group: String,
  description: String,
  tags: Map[String, String],
  value: F[AnyRef]
)
