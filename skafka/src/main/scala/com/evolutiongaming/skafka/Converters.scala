package com.evolutiongaming.skafka

import java.time.{Duration => DurationJ}
import java.util.{Collection => CollectionJ, Map => MapJ}

import com.evolutiongaming.nel.Nel
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => OffsetAndMetadataJ}
import org.apache.kafka.common.header.{Header => HeaderJ}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.common.{PartitionInfo => PartitionInfoJ, TopicPartition => TopicPartitionJ}

import scala.collection.JavaConverters._
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

    def asScala: ToBytes[A] = (a: A, topic: Topic) => self.serialize(topic, a)
  }


  implicit class DeserializerOps[A](val self: Deserializer[A]) extends AnyVal {

    def asScala: FromBytes[A] = (value: Bytes, topic: Topic) => self.deserialize(topic, value)
  }


  implicit class ToBytesOps[A](val self: ToBytes[A]) extends AnyVal {

    def asJava: Serializer[A] = new Serializer[A] {
      def configure(configs: MapJ[String, _], isKey: Boolean): Unit = {}

      def serialize(topic: Topic, a: A): Array[Byte] = self(a, topic)

      def close(): Unit = {}
    }
  }


  implicit class FromBytesOps[A](val self: FromBytes[A]) extends AnyVal {

    def asJava: Deserializer[A] = new Deserializer[A] {
      def configure(configs: MapJ[String, _], isKey: Boolean) = {}

      def deserialize(topic: Topic, bytes: Array[Byte]): A = self(bytes, topic)

      def close() = {}
    }
  }


  implicit class NelSkafkaOps[A](val self: Nel[A]) extends AnyVal {
    def asJava: CollectionJ[A] = self.toList.asJavaCollection
  }


  implicit final class DurationJOps(val duration: DurationJ) extends AnyVal {

    def asScala: scala.concurrent.duration.FiniteDuration = DurationConverters.toScala(duration)
  }


  implicit final class FiniteDurationOps(val duration: FiniteDuration) extends AnyVal {

    def asJava: java.time.Duration = DurationConverters.toJava(duration)
  }


  implicit class OffsetAndMetadataJOps(val self: OffsetAndMetadataJ) extends AnyVal {
    def asScala: OffsetAndMetadata = OffsetAndMetadata(self.offset(), self.metadata())
  }


  implicit class OffsetAndMetadataOps(val self: OffsetAndMetadata) extends AnyVal {
    def asJava: OffsetAndMetadataJ = new OffsetAndMetadataJ(self.offset, self.metadata)
  }
}