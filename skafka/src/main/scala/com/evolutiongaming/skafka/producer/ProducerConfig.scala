package com.evolutiongaming.skafka.producer

import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.CommonConfig
import com.typesafe.config.{Config, ConfigException}
import org.apache.kafka.clients.producer.{ProducerConfig => C}

import scala.concurrent.duration._

/**
  * Check [[http://kafka.apache.org/documentation/#producerconfigs]]
  */
final case class ProducerConfig(
  common: CommonConfig = CommonConfig.Default,
  batchSize: Int = 16384,
  acks: Acks = Acks.One,
  linger: FiniteDuration = 0.millis,
  maxRequestSize: Int = 1048576,
  maxBlock: FiniteDuration = 1.minute,
  bufferMemory: Long = 33554432L,
  compressionType: CompressionType = CompressionType.None,
  retries: Int = 0,
  maxInFlightRequestsPerConnection: Int = 5,
  partitionerClass: String = "org.apache.kafka.clients.producer.internals.DefaultPartitioner",
  interceptorClasses: List[String] = Nil,
  idempotence: Boolean = false,
  transactionTimeout: FiniteDuration = 1.minute,
  transactionalId: Option[String] = None) {

  def bindings: Map[String, String] = {
    val bindings = Map[String, String](
      (C.BATCH_SIZE_CONFIG, batchSize.toString),
      (C.ACKS_CONFIG, acks.names.head.toString),
      (C.LINGER_MS_CONFIG, linger.toMillis.toString),
      (C.MAX_REQUEST_SIZE_CONFIG, maxRequestSize.toString),
      (C.MAX_BLOCK_MS_CONFIG, maxBlock.toMillis.toString),
      (C.BUFFER_MEMORY_CONFIG, bufferMemory.toString),
      (C.COMPRESSION_TYPE_CONFIG, compressionType.toString.toLowerCase),
      (C.RETRIES_CONFIG, retries.toString),
      (C.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection.toString),
      (C.PARTITIONER_CLASS_CONFIG, partitionerClass),
      (C.INTERCEPTOR_CLASSES_CONFIG, interceptorClasses mkString ","),
      (C.ENABLE_IDEMPOTENCE_CONFIG, idempotence.toString),
      (C.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout.toMillis.toString)) ++
      transactionalId.map { (C.TRANSACTIONAL_ID_CONFIG, _) }

    bindings ++ common.bindings
  }

  def properties: java.util.Properties = {
    val properties = new java.util.Properties
    bindings foreach { case (k, v) => properties.put(k, v) }
    properties
  }
}

object ProducerConfig {

  lazy val Default: ProducerConfig = ProducerConfig()

  private implicit val CompressionTypeFromConf = FromConf[CompressionType] { (conf, path) =>
    val str = conf.getString(path)
    val value = CompressionType.Values.find { _.toString equalsIgnoreCase str }
    value getOrElse {
      throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse CompressionType from $str")
    }
  }

  private implicit val AcksFromConf = FromConf[Acks] { (conf, path) =>
    val str = conf.getString(path)

    val values = for {
      value <- Acks.Values
      name <- value.names.toList
      if name equalsIgnoreCase str
    } yield value

    values.headOption getOrElse {
      throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse Acks from $str")
    }
  }

  def apply(config: Config): ProducerConfig = {

    def get[T: FromConf](path: String, paths: String*) = {
      config.getOpt[T](path, paths: _*)
    }

    def getDuration(path: String, pathMs: => String) = {
      val value = try get[FiniteDuration](path) catch { case _: ConfigException => None }
      value orElse get[Long](pathMs).map { _.millis }
    }

    ProducerConfig(
      common = CommonConfig(config),
      acks = get[Acks]("acks") getOrElse Default.acks,
      bufferMemory = get[Long](
        "buffer-memory",
        "buffer.memory") getOrElse Default.bufferMemory,
      compressionType = get[CompressionType](
        "compression-type",
        "compression.type") getOrElse Default.compressionType,
      retries = get[Int]("retries") getOrElse Default.retries,
      batchSize = get[Int](
        "batch-size",
        "batch.size") getOrElse Default.batchSize,
      linger = getDuration(
        "linger",
        "linger.ms") getOrElse Default.linger,
      maxBlock = getDuration(
        "max-block",
        "max.block.ms") getOrElse Default.maxBlock,
      maxRequestSize = get[Int](
        "max-request-size",
        "max.request.size") getOrElse Default.maxRequestSize,
      maxInFlightRequestsPerConnection = get[Int](
        "max-in-flight-requests-per-connection",
        "max.in.flight.requests.per.connection") getOrElse Default.maxInFlightRequestsPerConnection,
      partitionerClass = get[String](
        "partitioner-class",
        "partitioner.class") getOrElse Default.partitionerClass,
      interceptorClasses = get[List[String]](
        "interceptor-classes",
        "interceptor.classes") getOrElse Default.interceptorClasses,
      idempotence = get[Boolean](
        "idempotence",
        "enable-idempotence",
        "enable.idempotence") getOrElse Default.idempotence,
      transactionTimeout = getDuration(
        "transaction-timeout",
        "transaction.timeout.ms") getOrElse Default.transactionTimeout,
      transactionalId = get[String](
        "transactional-id",
        "transactional.id") orElse Default.transactionalId)
  }

  implicit def nelFromConf[T](implicit fromConf: FromConf[List[T]]): FromConf[Nel[T]] = {
    FromConf { case (config, path) =>
      val list = fromConf(config, path)
      Nel.unsafe(list)
    }
  }
}