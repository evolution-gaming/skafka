package com.evolutiongaming.skafka.consumer

import cats.instances.try_._
import cats.kernel.Eq
import cats.laws.discipline.MonadTests
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.skafka.consumer.RebalanceCallbackLawsSpec.runWithMock
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

import scala.util.{Failure, Success, Try}

class RebalanceCallbackLawsSpec extends FunSuiteDiscipline with AnyFunSuiteLike with Configuration {
  // Generate each of ADT members. Where possible, generate a successful outcome as well as an unsuccessful one.
  // Since ADT members are private, we can't create them directly and in some cases that makes us generate
  // composite members (see generation for WithConsumer)
  implicit def arbAny[A](implicit A: Arbitrary[A]): Arbitrary[RebalanceCallback[Try, A]] = Arbitrary(Gen.oneOf(
    A.arbitrary.map(RebalanceCallback.pure), // Pure
    A.arbitrary.map(a => RebalanceCallback.pure(a).flatMap(b => RebalanceCallback.pure(b))), // Bind
    A.arbitrary.map(a => RebalanceCallback.lift(Success(a))), // Lift
    Gen.const(RebalanceCallback.lift(Failure(new Exception("Lift fail")))), // Lift
    A.arbitrary.map(a => RebalanceCallback.commit.map(_ => a)), // WithConsumer
    Gen.const(RebalanceCallback.fromTry(Failure(new Exception("Test")))) // Error
  ))

  implicit def eq[A: Eq]: Eq[RebalanceCallback[Try, A]] = new Eq[RebalanceCallback[Try, A]] {
    implicit val eqT: Eq[Throwable] = Eq.allEqual

    override def eqv(x: RebalanceCallback[Try, A], y: RebalanceCallback[Try, A]): Boolean =
      Eq[Try[A]].eqv(runWithMock(x), runWithMock(y))
  }

  checkAll("RebalanceCallback.MonadLaws", MonadTests[RebalanceCallback[Try, *]].monad[Int, Int, Int])
}

object RebalanceCallbackLawsSpec {
  val consumerMock = new ExplodingConsumer

  def runWithMock[F[_] : ToTry, A](rc: RebalanceCallback[F, A]) = RebalanceCallback.run(rc, consumerMock)
}
