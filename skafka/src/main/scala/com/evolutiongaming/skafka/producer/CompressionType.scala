package com.evolutiongaming.skafka.producer

sealed trait CompressionType extends Product

object CompressionType {
  val Values: Set[CompressionType] = Set(None, Gzip, Snappy, Lz4)

  case object None extends CompressionType
  case object Gzip extends CompressionType
  case object Snappy extends CompressionType
  case object Lz4 extends CompressionType
}
