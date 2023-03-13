package com.evolutiongaming.skafka.producer

import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.skafka.{CommonConfig, SaslSupportConfig, SslSupportConfig}
import com.typesafe.config.{Config, ConfigException}
import org.apache.kafka.clients.producer.{Partitioner, ProducerConfig => C}

import scala.concurrent.duration._
import scala.util.Try

/**
  * Check [[http://kafka.apache.org/documentation/#producerconfigs]]
  */
final case class ProducerConfig(
  common: CommonConfig                              = CommonConfig.Default,
  batchSize: Int                                    = 16384,
  deliveryTimeout: FiniteDuration                   = 2.minutes,
  acks: Acks                                        = Acks.All,
  linger: FiniteDuration                            = 0.millis,
  maxRequestSize: Int                               = 1048576,
  maxBlock: FiniteDuration                          = 1.minute,
  bufferMemory: Long                                = 33554432L,
  compressionType: CompressionType                  = CompressionType.None,
  retries: Int                                      = Int.MaxValue,
  maxInFlightRequestsPerConnection: Int             = 5,
  partitionerClass: Option[Class[_ <: Partitioner]] = None,
  interceptorClasses: List[String]                  = Nil,
  idempotence: Boolean                              = true,
  transactionTimeout: FiniteDuration                = 1.minute, // TODO delete
  transactionalId: Option[String]                   = None,
  saslSupport: SaslSupportConfig                    = SaslSupportConfig.Default,
  sslSupport: SslSupportConfig                      = SslSupportConfig.Default,
  partitionerIgnoreKeys: Boolean                    = false,
  partitionerAdaptivePartitioningEnable: Boolean    = true,
  partitionerAvailabilityTimeout: FiniteDuration    = 0.seconds,
  metadataMaxIdle: FiniteDuration                   = 5.minutes,
) {

  def bindings: Map[String, AnyRef] = {
    val bindings1 = Map[String, String](
      (C.BATCH_SIZE_CONFIG, batchSize.toString),
      (C.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout.toMillis.toString),
      (C.ACKS_CONFIG, acks.names.head),
      (C.LINGER_MS_CONFIG, linger.toMillis.toString),
      (C.MAX_REQUEST_SIZE_CONFIG, maxRequestSize.toString),
      (C.MAX_BLOCK_MS_CONFIG, maxBlock.toMillis.toString),
      (C.BUFFER_MEMORY_CONFIG, bufferMemory.toString),
      (C.COMPRESSION_TYPE_CONFIG, compressionType.toString.toLowerCase),
      (C.RETRIES_CONFIG, retries.toString),
      (C.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection.toString),
      (C.INTERCEPTOR_CLASSES_CONFIG, interceptorClasses mkString ","),
      (C.ENABLE_IDEMPOTENCE_CONFIG, idempotence.toString),
      (C.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout.toMillis.toString),
      (C.PARTITIONER_IGNORE_KEYS_CONFIG, partitionerIgnoreKeys.toString),
      (C.PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG, partitionerAdaptivePartitioningEnable.toString),
      (C.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG, partitionerAvailabilityTimeout.toMillis.toString),
      (C.METADATA_MAX_IDLE_CONFIG, metadataMaxIdle.toMillis.toString),
    )

    val bindings = bindings1 ++
      transactionalId.map { (C.TRANSACTIONAL_ID_CONFIG, _) }
    partitionerClass.map { value => (C.PARTITIONER_CLASS_CONFIG, value) }

    bindings ++ common.bindings ++ saslSupport.bindings ++ sslSupport.bindings
  }

  def properties: java.util.Properties = {
    val properties = new java.util.Properties
    bindings foreach { case (k, v) => properties.put(k, v) }
    properties
  }
}

object ProducerConfig {

  val Default: ProducerConfig = ProducerConfig()

  private implicit val CompressionTypeFromConf = FromConf[CompressionType] { (conf, path) =>
    val str   = conf.getString(path)
    val value = CompressionType.Values.find { _.toString equalsIgnoreCase str }
    value getOrElse {
      throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse CompressionType from $str")
    }
  }

  private implicit val AcksFromConf = FromConf[Acks] { (conf, path) =>
    val str = conf.getString(path)

    val values = Acks.Values.filter(_.names.exists(str.equalsIgnoreCase))

    values.headOption getOrElse {
      throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse Acks from $str")
    }
  }

  def apply(config: Config): ProducerConfig = {
    apply(config, Default)
  }

  def apply(config: Config, default: => ProducerConfig): ProducerConfig = {

    def get[T: FromConf](path: String, paths: String*) = {
      config.getOpt[T](path, paths: _*)
    }

    def getDuration(path: String, pathMs: => String) = {
      val value =
        try get[FiniteDuration](path)
        catch { case _: ConfigException => None }
      value orElse get[Long](pathMs).map { _.millis }
    }

    val partitionerClass = {

      def classOf(name: String) = {

        def classOfClassLoader = {
          val thread      = Thread.currentThread()
          val classLoader = thread.getContextClassLoader
          Try { classLoader.loadClass(name) }
        }

        def classOf = {
          Try { Class.forName(name) }
        }

        for {
          a <- classOfClassLoader orElse classOf
          a <- Try { a.asInstanceOf[Class[Partitioner]] }
        } yield a
      }

      for {
        name             <- get[String]("partitioner-class", "partitioner.class")
        partitionerClass <- classOf(name).toOption
      } yield partitionerClass
    }

    ProducerConfig(
      common          = CommonConfig(config, default.common),
      acks            = get[Acks]("acks") getOrElse default.acks,
      bufferMemory    = get[Long]("buffer-memory", "buffer.memory") getOrElse default.bufferMemory,
      compressionType = get[CompressionType]("compression-type", "compression.type") getOrElse default.compressionType,
      retries         = get[Int]("retries") getOrElse default.retries,
      batchSize       = get[Int]("batch-size", "batch.size") getOrElse default.batchSize,
      deliveryTimeout = getDuration("delivery-timeout", "delivery.timeout.ms") getOrElse default.deliveryTimeout,
      linger          = getDuration("linger", "linger.ms") getOrElse default.linger,
      maxBlock        = getDuration("max-block", "max.block.ms") getOrElse default.maxBlock,
      maxRequestSize  = get[Int]("max-request-size", "max.request.size") getOrElse default.maxRequestSize,
      maxInFlightRequestsPerConnection = get[Int](
        "max-in-flight-requests-per-connection",
        "max.in.flight.requests.per.connection"
      ) getOrElse default.maxInFlightRequestsPerConnection,
      partitionerClass = partitionerClass orElse default.partitionerClass,
      interceptorClasses =
        get[List[String]]("interceptor-classes", "interceptor.classes") getOrElse default.interceptorClasses,
      idempotence =
        get[Boolean]("idempotence", "enable-idempotence", "enable.idempotence") getOrElse default.idempotence,
      transactionTimeout =
        getDuration("transaction-timeout", "transaction.timeout.ms") getOrElse default.transactionTimeout,
      transactionalId = get[String]("transactional-id", "transactional.id") orElse default.transactionalId,
      saslSupport     = SaslSupportConfig(config, default.saslSupport),
      sslSupport      = SslSupportConfig(config),
      partitionerIgnoreKeys =
        get[Boolean]("partitioner-ignore-keys", "partitioner.ignore.keys") getOrElse default.partitionerIgnoreKeys,
      partitionerAdaptivePartitioningEnable =
        get[Boolean]("partitioner-adaptive-partitioning-enable", "partitioner.adaptive.partitioning.enable")
          .getOrElse(default.partitionerAdaptivePartitioningEnable),
      partitionerAvailabilityTimeout =
        getDuration("partitioner-availability-timeout", "partitioner.availability.timeout.ms").getOrElse(
          default.partitionerAvailabilityTimeout
        ),
      metadataMaxIdle = getDuration("metadata-max-idle", "metadata.max.idle.ms").getOrElse(default.metadataMaxIdle),
    )
  }

  //for binary compatibility
  private[producer] def apply(
    common: CommonConfig,
    batchSize: Int,
    deliveryTimeout: FiniteDuration,
    acks: Acks,
    linger: FiniteDuration,
    maxRequestSize: Int,
    maxBlock: FiniteDuration,
    bufferMemory: Long,
    compressionType: CompressionType,
    retries: Int,
    maxInFlightRequestsPerConnection: Int,
    partitionerClass: Option[Class[_ <: Partitioner]],
    interceptorClasses: List[String],
    idempotence: Boolean,
    transactionTimeout: FiniteDuration,
    transactionalId: Option[String],
    saslSupport: SaslSupportConfig,
  ): ProducerConfig = new ProducerConfig(
    common                           = common,
    batchSize                        = batchSize,
    deliveryTimeout                  = deliveryTimeout,
    acks                             = acks,
    linger                           = linger,
    maxRequestSize                   = maxRequestSize,
    maxBlock                         = maxBlock,
    bufferMemory                     = bufferMemory,
    compressionType                  = compressionType,
    retries                          = retries,
    maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection,
    partitionerClass                 = partitionerClass,
    interceptorClasses               = interceptorClasses,
    idempotence                      = idempotence,
    transactionTimeout               = transactionTimeout,
    transactionalId                  = transactionalId,
    saslSupport                      = saslSupport,
  )

  //for binary compatibility
  private[producer] def apply(
    common: CommonConfig,
    batchSize: Int,
    deliveryTimeout: FiniteDuration,
    acks: Acks,
    linger: FiniteDuration,
    maxRequestSize: Int,
    maxBlock: FiniteDuration,
    bufferMemory: Long,
    compressionType: CompressionType,
    retries: Int,
    maxInFlightRequestsPerConnection: Int,
    partitionerClass: Option[Class[_ <: Partitioner]],
    interceptorClasses: List[String],
    idempotence: Boolean,
    transactionTimeout: FiniteDuration,
    transactionalId: Option[String],
  ): ProducerConfig = new ProducerConfig(
    common                           = common,
    batchSize                        = batchSize,
    deliveryTimeout                  = deliveryTimeout,
    acks                             = acks,
    linger                           = linger,
    maxRequestSize                   = maxRequestSize,
    maxBlock                         = maxBlock,
    bufferMemory                     = bufferMemory,
    compressionType                  = compressionType,
    retries                          = retries,
    maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection,
    partitionerClass                 = partitionerClass,
    interceptorClasses               = interceptorClasses,
    idempotence                      = idempotence,
    transactionTimeout               = transactionTimeout,
    transactionalId                  = transactionalId,
  )
}
