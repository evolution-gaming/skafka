package com.evolutiongaming.skafka

import cats.effect.IO
import org.scalatest.matchers.{MatchResult, Matcher}
import com.evolutiongaming.skafka.IOSuite._

trait IOMatchers {
  class IOResultMatcher[-T](expected: T) extends Matcher[IO[T]] {
    def apply(left: IO[T]): MatchResult =
      MatchResult(
        matches                  = left.unsafeRunSync() == expected,
        rawFailureMessage        = s"IO result didn't equal $expected",
        rawNegatedFailureMessage = s"IO result equals $expected"
      )
  }

  def produce[T](expected: T) = new IOResultMatcher[T](expected)

  def verify[T, U](job: IO[T])(check: T => U): U = {
    check(job.unsafeRunSync())
  }

}
object IOMatchers extends IOMatchers
