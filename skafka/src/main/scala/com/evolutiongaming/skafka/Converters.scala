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

    def asScala: ToBytes[A] = new ToBytes[A] {
      def apply(a: A, topic: Topic): Bytes = self.serialize(topic, a)
    }
  }


  implicit class DeserializerOps[A](val self: Deserializer[A]) extends AnyVal {

    def asScala: FromBytes[A] = new FromBytes[A] {
      def apply(value: Bytes, topic: Topic): A = self.deserialize(topic, value)
    }
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
}