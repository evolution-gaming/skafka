package com.evolutiongaming.skafka.consumer

import java.util.concurrent.atomic.AtomicReference

import cats.Applicative
import cats.data.{NonEmptySet => Nes}
import cats.effect.IO
import cats.implicits._
import com.evolutiongaming.skafka.consumer.DataPoints._
import com.evolutiongaming.skafka.consumer.RebalanceListener1Spec._
import com.evolutiongaming.skafka.{Offset, TopicPartition}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.jdk.CollectionConverters._
import scala.util.Try

class RebalanceListener1Spec extends AnyFreeSpec with Matchers {
  "scala version of ConsumerRebalanceListener's documentation example is working" in {
    val seekResult: AtomicReference[List[String]] = new AtomicReference(List.empty)

    val listener1 = new SaveOffsetsOnRebalance[IO]

    val consumer = new ExplodingConsumer {
      override def seek(partition: TopicPartitionJ, offset: Long): Unit = {
        val _ = seekResult.getAndUpdate(_ :+ partition.toString)
      }

      override def position(partition: TopicPartitionJ): Long = {
        offsetsMap.j.get(partition)
      }
    }.asRebalanceConsumer

    listener1.onPartitionsAssigned(partitions.s).run(consumer) mustBe Try(())
    seekResult.get() must contain theSameElementsAs partitions.j.asScala.map(_.toString)

    listener1.onPartitionsRevoked(partitions.s).run(consumer) mustBe Try(())

    listener1.onPartitionsLost(partitions.s).effectAs[IO].run(consumer) mustBe Try(())

  }
}

object RebalanceListener1Spec {

  /*
   *   public class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
   *       private Consumer<?,?> consumer;
   *
   *       public SaveOffsetsOnRebalance(Consumer<?,?> consumer) {
   *           this.consumer = consumer;
   *       }
   *
   *       public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
   *           // save the offsets in an external store using some custom code not described here
   *           for(TopicPartition partition: partitions)
   *              saveOffsetInExternalStore(consumer.position(partition));
   *       }
   *
   *       public void onPartitionsLost(Collection<TopicPartition> partitions) {
   *           // do not need to save the offsets since these partitions are probably owned by other consumers already
   *       }
   *
   *       public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
   *           // read the offsets from an external store using some custom code not described here
   *           for(TopicPartition partition: partitions)
   *              consumer.seek(partition, readOffsetFromExternalStore(partition));
   *       }
   *   }
   * }
   */
  class SaveOffsetsOnRebalance[F[_]: Applicative] extends RebalanceListener1WithConsumer[F] {

    // import is needed to use `fa.lift` syntax where
    // `fa: F[A]`
    // `fa.lift: RebalanceCallback[F, A]`
    import RebalanceCallback.syntax._

    def onPartitionsAssigned(partitions: Nes[TopicPartition]) =
      for {
        // read the offsets from an external store using some custom code not described here
        offsets <- readOffsetsFromExternalStore[F](partitions).lift
        a       <- offsets.toList.foldMapM { case (partition, offset) => consumer.seek(partition, offset) }
      } yield a

    def onPartitionsRevoked(partitions: Nes[TopicPartition]) =
      for {
        positions <- partitions.foldM(Map.empty[TopicPartition, Offset]) {
          case (offsets, partition) =>
            for {
              position <- consumer.position(partition)
            } yield offsets + (partition -> position)
        }
        // save the offsets in an external store using some custom code not described here
        a <- saveOffsetsInExternalStore[F](positions).lift
      } yield a

    // do not need to save the offsets since these partitions are probably owned by other consumers already
    def onPartitionsLost(partitions: Nes[TopicPartition]) = RebalanceCallback.empty
  }

  def readOffsetsFromExternalStore[F[_]: Applicative](
    partitions: Nes[TopicPartition]
  ): F[Map[TopicPartition, Offset]] = {
    partitions
      .foldLeft(Map.empty[TopicPartition, Offset]) {
        case (agg, partition) => agg + (partition -> offsetsMap.s.toSortedMap(partition))
      }
      .pure[F]
  }

  def saveOffsetsInExternalStore[F[_]: Applicative](offsets: Map[TopicPartition, Offset]): F[Unit] = {
    if (offsets == offsetsMap.s.toSortedMap) ().pure[F]
    else sys.error("saving wrong offsets")
  }

}
