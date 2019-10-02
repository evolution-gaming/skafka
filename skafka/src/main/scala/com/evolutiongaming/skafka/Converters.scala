package com.evolutiongaming.skafka

import java.time.{Duration => DurationJ}
import java.util.{Collection => CollectionJ, Map => MapJ}

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.catshelper.{FromTry, ToTry}
import com.evolutiongaming.catshelper.EffectHelper._
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.common.header.{Header => HeaderJ}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.common.{PartitionInfo => PartitionInfoJ, TopicPartition => TopicPartitionJ}

import scala.jdk.CollectionConverters._
import scala.compat.java8.DurationConverters
import scala.concurrent.duration.FiniteDuration

object Converters {

  implicit class HeaderOps(val self: Header) extends AnyVal {

    def asJava: HeaderJ = new HeaderJ {
      def value = self.value

      def key = self.key
    }
  }


  implicit class HeaderJOps(val self: HeaderJ) extends AnyVal {
    def asScala: Header = Header(self.key(), self.value())
  }


  implicit class AnyOps[A](val self: A) extends AnyVal {
    def noneIf(x: A): Option[A] = if (self == x) None else Some(self)
  }


  implicit class TopicPartitionOps(val self: TopicPartition) extends AnyVal {
    def asJava: TopicPartitionJ = new TopicPartitionJ(self.topic, self.partition)
  }


  implicit class TopicPartitionJOps(val self: TopicPartitionJ) extends AnyVal {
    def asScala: TopicPartition = TopicPartition(self.topic(), self.partition())
  }


  implicit class PartitionInfoJOps(val self: PartitionInfoJ) extends AnyVal {

    def asScala: PartitionInfo = {
      PartitionInfo(
        topicPartition = TopicPartition(self.topic, self.partition),
        leader = self.leader,
        replicas = self.replicas.toList,
        inSyncReplicas = self.inSyncReplicas.toList,
        offlineReplicas = self.offlineReplicas.toList)
    }
  }

  implicit class PartitionInfoOps(val self: PartitionInfo) extends AnyVal {

    def asJava: PartitionInfoJ = {
      new PartitionInfoJ(
        self.topic,
        self.partition,
        self.leader,
        self.replicas.toArray,
        self.inSyncReplicas.toArray,
        self.offlineReplicas.toArray)
    }
  }


  implicit class MapJOps[K, V](val self: MapJ[K, V]) extends AnyVal {

    def asScalaMap[KK, VV](kf: K => KK, vf: V => VV): Map[KK, VV] = {
      val zero = Map.empty[KK, VV]
      self.asScala.foldLeft(zero) { case (map, (k, v)) =>
        val kk = kf(k)
        val vv = vf(v)
        map.updated(kk, vv)
      }
    }
  }


  implicit class MapOps[K, V](val self: Map[K, V]) extends AnyVal {

    def asJavaMap[KK, VV](kf: K => KK, vf: V => VV): MapJ[KK, VV] = {
      val zero = new java.util.HashMap[KK, VV]()
      self.foldLeft(zero) { case (map, (k, v)) =>
        val kk = kf(k)
        val vv = vf(v)
        map.put(kk, vv)
        map
      }
    }
  }


  implicit class SerializerOps[A](val self: Serializer[A]) extends AnyVal {

    def asScala[F[_] : FromTry]: ToBytes[F, A] = (a: A, topic: Topic) => {
      FromTry[F].unsafe { self.serialize(topic, a) }
    }
  }


  implicit class DeserializerOps[A](val self: Deserializer[A]) extends AnyVal {

    def asScala[F[_] : FromTry]: FromBytes[F, A] = (value: Bytes, topic: Topic) => {
      FromTry[F].unsafe { self.deserialize(topic, value) }
    }
  }


  implicit class ToBytesOps[F[_], A](val self: ToBytes[F, A]) extends AnyVal {

    def asJava(implicit toTry: ToTry[F]): Serializer[A] = new Serializer[A] {
      override def configure(configs: MapJ[String, _], isKey: Boolean): Unit = {}

      def serialize(topic: Topic, a: A): Array[Byte] = self(a, topic).toTry.get

      override def close() = {}
    }
  }


  implicit class SkafkaFromBytesOps[F[_], A](val self: FromBytes[F, A]) extends AnyVal {

    def asJava(implicit toTry: ToTry[F]): Deserializer[A] = new Deserializer[A] {
      override def configure(configs: MapJ[String, _], isKey: Boolean) = {}

      def deserialize(topic: Topic, bytes: Array[Byte]): A = self(bytes, topic).toTry.get

      override def close() = {}
    }
  }


  implicit class SkafkaNelOps[A](val self: Nel[A]) extends AnyVal {
    def asJava: CollectionJ[A] = self.toList.asJavaCollection
  }


  implicit final class SkafkaDurationJOps(val duration: DurationJ) extends AnyVal {

    def asScala: scala.concurrent.duration.FiniteDuration = DurationConverters.toScala(duration)
  }


  implicit final class SkafkaFiniteDurationOps(val duration: FiniteDuration) extends AnyVal {

    def asJava: java.time.Duration = DurationConverters.toJava(duration)
  }


  implicit class SkafkaOffsetAndMetadataJOps(val self: OffsetAndMetadataJ) extends AnyVal {
    def asScala: OffsetAndMetadata = OffsetAndMetadata(self.offset(), self.metadata())
  }


  implicit class SkafkaOffsetAndMetadataOps(val self: OffsetAndMetadata) extends AnyVal {
    def asJava: OffsetAndMetadataJ = new OffsetAndMetadataJ(self.offset, self.metadata)
  }
}