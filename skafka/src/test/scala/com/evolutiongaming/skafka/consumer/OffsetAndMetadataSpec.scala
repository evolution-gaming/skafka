package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka.OffsetAndMetadata
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OffsetAndMetadataSpec extends AnyFunSuite with Matchers {

  for {
    (value, expected) <- List(
      (OffsetAndMetadata.empty, "OffsetAndMetadata(0)"),
      (OffsetAndMetadata(1, "2"), "OffsetAndMetadata(1,2)"))
  } {
    test(s"$value.toString") {
      value.toString shouldEqual expected
    }
  }
}
