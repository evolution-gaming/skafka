package com.evolutiongaming.skafka

import java.util.{Map => MapJ, Collection => CollectionJ}

import com.evolutiongaming.nel.Nel
import org.apache.kafka.common.header.{Header => JHeader}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.apache.kafka.common.{PartitionInfo => PartitionInfoJ, TopicPartition => TopicPartitionJ}

import scala.collection.JavaConverters._

object Converters {

  implicit class HeaderOps(val self: Header) extends AnyVal {

    def asJava: JHeader = new JHeader {
      def value = self.value
      def key = self.key
    }
  }

  
  implicit class JHeaderOps(val self: JHeader) extends AnyVal {
    def asScala: Header = Header(self.key(), self.value())
  }


  implicit class AnyOps[T](val self: T) extends AnyVal {
    def noneIf(x: T): Option[T] = if (self == x) None else Some(self)
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


  implicit class SerializerOps[T](val self: Serializer[T]) extends AnyVal {

    def asScala: ToBytes[T] = new ToBytes[T] {
      def apply(value: T, topic: Topic): Bytes = self.serialize(topic, value)
    }
  }


  implicit class DeserializerOps[T](val self: Deserializer[T]) extends AnyVal {

    def asScala: FromBytes[T] = new FromBytes[T] {
      def apply(value: Bytes, topic: Topic): T = self.deserialize(topic, value)
    }
  }


  implicit class ToBytesOps[T](val self: ToBytes[T]) extends AnyVal {

    def asJava: Serializer[T] = new Serializer[T] {
      def configure(configs: MapJ[Metadata, _], isKey: Boolean): Unit = {}
      def serialize(topic: Metadata, data: T): Array[Byte] = self(data, topic)
      def close(): Unit = {}
    }
  }


  implicit class FromBytesOps[T](val self: FromBytes[T]) extends AnyVal {

    def asJava: Deserializer[T] = new Deserializer[T] {
      def configure(configs: MapJ[String, _], isKey: Boolean) = {}
      def deserialize(topic: Topic, data: Array[Byte]): T = self(data, topic)
      def close() = {}
    }
  }


  implicit class NelSkafkaOps[T](val self: Nel[T]) extends AnyVal {
    def asJava: CollectionJ[T] = self.toList.asJavaCollection
  }
}