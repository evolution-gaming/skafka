package com.evolutiongaming.skafka.consumer

import java.{util => ju}

import cats.effect.IO
import cats.instances.either._
import cats.instances.try_._
import cats.kernel.Eq
import cats.laws.discipline.MonadErrorTests
import com.evolutiongaming.catshelper.{MonadThrowable, ToTry}
import org.apache.kafka.common.PartitionInfo
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

import scala.util.control.NoStackTrace
import scala.util.{Failure, Try}

class RebalanceCallbackLawsSpec extends FunSuiteDiscipline with AnyFunSuiteLike with Configuration {

  import RebalanceCallback._
  import RebalanceCallbackLawsSpec._

  // Generate each of ADT members. Where possible, generate a successful outcome as well as an unsuccessful one.
  // Since ADT members are private, we can't create them directly and in some cases that makes us generate
  // composite members (see generation for WithConsumer)
  implicit def arbAny[A](implicit A: Arbitrary[A]): Arbitrary[RebalanceCallback[IO, A]] = Arbitrary(
    Gen.oneOf(
      A.arbitrary.map(pure), // Pure
      A.arbitrary.map(a => pure(a).flatMap(b => pure(b))), // Bind
      A.arbitrary.map(a => lift(IO.pure(a))), // Lift(Success)
      Gen.const(lift(IO.raiseError(new Exception("Lift fail") with NoStackTrace))), // Lift(Error)
      A.arbitrary.map(a => commit.map(_ => a)), // WithConsumer with success
      A.arbitrary.map(a => topics.map(_ => a)), // WithConsumer with failure
      Gen.const(fromTry(Failure(new Exception("Test") with NoStackTrace))), // Error(e)
      A.arbitrary.map(a => pure(a).handleErrorWith(_ => pure(a))), // HandleErrorWith
      A.arbitrary.map(a => pure(a).handleErrorWith(_ => fromTry(Failure(new Exception("Handle error fail"))))),
    )
  )

  implicit val eqThrowable: Eq[Throwable] = Eq.allEqual

  // Test with RebalanceConsumer.run
  {
    implicit def eq[A: Eq]: Eq[RebalanceCallback[IO, A]] = new Eq[RebalanceCallback[IO, A]] {
      override def eqv(x: RebalanceCallback[IO, A], y: RebalanceCallback[IO, A]): Boolean =
        Eq[Try[A]].eqv(runWithMock(x), runWithMock(y))
    }

    checkAll(
      "RebalanceCallback.MonadErrorLaws.run",
      MonadErrorTests[RebalanceCallback[IO, *], Throwable].monadError[Int, Int, Int]
    )
  }

  // Test with RebalanceConsumer.toF[IO]
  {
    implicit def eq[A: Eq]: Eq[RebalanceCallback[IO, A]] = new Eq[RebalanceCallback[IO, A]] {
      implicit val eqIO: Eq[IO[A]] = (x, y) => {
        Eq[Either[Throwable, A]].eqv(x.attempt.unsafeRunSync(), y.attempt.unsafeRunSync())
      }

      override def eqv(x: RebalanceCallback[IO, A], y: RebalanceCallback[IO, A]): Boolean =
        Eq[IO[A]].eqv(toFWithMock(x), toFWithMock(y))
    }

    checkAll(
      "RebalanceCallback.MonadErrorLaws.toF",
      MonadErrorTests[RebalanceCallback[IO, *], Throwable].monadError[Int, Int, Int]
    )
  }
}

object RebalanceCallbackLawsSpec {
  val consumerMock = new ExplodingConsumer {
    override def commitSync(): Unit = ()

    override def listTopics(): ju.Map[String, ju.List[PartitionInfo]] = ExplodingConsumer.notImplemented
  }

  def toFWithMock[F[_]: MonadThrowable, A](rc: RebalanceCallback[F, A]) = rc.toF(RebalanceConsumer(consumerMock))

  def runWithMock[F[_]: ToTry, A](rc: RebalanceCallback[F, A]) = rc.run(RebalanceConsumer(consumerMock))
}
