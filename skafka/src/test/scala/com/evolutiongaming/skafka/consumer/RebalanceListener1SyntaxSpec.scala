package com.evolutiongaming.skafka.consumer

import cats.data.{NonEmptySet => Nes}
import cats.effect.IO
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.skafka.consumer.DataPoints._
import com.evolutiongaming.skafka.consumer.RebalanceListener1SyntaxSpec._
import org.apache.kafka.clients.consumer.{MockConsumer, OffsetResetStrategy}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.util.Try
class RebalanceListener1SyntaxSpec extends AnyFreeSpec with Matchers {

  "type inference to the max" in {
    val consumer   = RebalanceConsumerJ(new MockConsumer(OffsetResetStrategy.NONE))
    val tfListener = new TfRebalanceListener1[IO]

    RebalanceCallback
      .run(tfListener.onPartitionsAssigned(partitions.s).effectAs[IO], consumer) mustBe Try(())
  }

}

object RebalanceListener1SyntaxSpec {
  // TODO: add complex example show casing better type inference with RebalanceCallback.api[F]
  class TfRebalanceListener1[F[_]] extends RebalanceListener1[F] {
    def onPartitionsAssigned(partitions: Nes[TopicPartition]) = RebalanceCallback.empty
    def onPartitionsRevoked(partitions: Nes[TopicPartition])  = RebalanceCallback.empty
    def onPartitionsLost(partitions: Nes[TopicPartition])     = RebalanceCallback.empty
  }
}
