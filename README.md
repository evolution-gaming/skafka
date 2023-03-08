# Skafka
[![Build Status](https://github.com/evolution-gaming/skafka/workflows/CI/badge.svg)](https://github.com/evolution-gaming/skafka/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/skafka/badge.svg?branch=master)](https://coveralls.io/github/evolution-gaming/skafka?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/faac7c4d0b924320b60ce9eefc360b12)](https://www.codacy.com/app/evolution-gaming/skafka?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/skafka&amp;utm_campaign=Badge_Grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolutiongaming&a=skafka_2.13&repos=public)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

Scala wrapper for [kafka-clients v2.7.1](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/2.7.1)

## Key features

1. It provides null-less Scala apis for [Producer](skafka/src/main/scala/com/evolutiongaming/skafka/producer/Producer.scala) & [Consumer](skafka/src/main/scala/com/evolutiongaming/skafka/consumer/Consumer.scala)

2. Makes it easy to use your effect monad with help of [cats-effect](https://typelevel.org/cats-effect/)

3. Blocking calls are being executed on provided `ExecutionContext`.

4. Simple `case class` based configuration

5. Support of [typesafe config](https://github.com/lightbend/config)    


## Producer usage example

```scala
val producer = Producer.of[IO](config, ecBlocking)
val metadata: IO[RecordMetadata] = producer.use { producer =>
  val record = ProducerRecord(topic = "topic", key = "key", value = "value") 
  producer.send(record).flatten 
}
```

## Consumer usage example

```scala
val consumer = Consumer.of[IO, String, String](config, ecBlocking)
val records: IO[ConsumerRecords[String, String]] = consumer.use { consumer => 
  for {
    _       <- consumer.subscribe(Nel("topic"), None)
    records <- consumer.poll(100.millis)
  } yield records 
}
```

## Collecting metrics
Both `Producer` and `Consumer` expose metrics, internally collected by the Java client via `clientMetrics` method.  
To simplify collection of metrics from multiple clients inside the same VM you can use `KafkaMetricsRegistry`.
It allows 'registering' functions that obtain metrics from different clients, aggregating them into a single list
of metrics when collected. This allows defining clients in different code units with the only requirement of registering
them in `KafkaMetricsRegistry`. The registered functions will be saved in a `Ref` and invoked every time metrics
are collected.  
Examples:
1. Manual registration of each client
```scala
import com.evolutiongaming.skafka.consumer.ConsumerOf
import com.evolutiongaming.skafka.metrics.KafkaMetricsRegistry
import com.evolutiongaming.skafka.producer.ProducerOf

val consumerOf: ConsumerOf[F] = ???
val producerOf: ProducerOf[F] = ???
val kafkaRegistry: KafkaMetricsRegistry[F] = ???

for {
  consumer <- consumerOf.apply(consumerConfig)
           <- kafkaRegistry.register(consumer.clientMetrics)
  producer <- producerOf.apply(producerConfig)
  _        <- kafkaRegistry.register(producer.clientMetrics)
  
  metrics  <- kafkaRegistry.collectAll
} yield ()
```
2. Wrapping `ConsumerOf` or `ProducerOf` with a syntax extension
```scala
import com.evolutiongaming.skafka.metrics.syntax._

val kafkaRegistry: KafkaMetricsRegistry[F] = ...
val consumerOf: ConsumerOf[F] = ConsumerOf.apply1[F](...).withNativeMetrics(kafkaRegistry)
val producerOf: ProducerOf[F] = ProducerOf.apply1[F](...).withNativeMetrics(kafkaRegistry)

for {
  consumer <- consumerOf.apply(consumerConfig)
  producer <- producerOf.apply(producerConfig)
  metrics  <- kafkaRegistry.collectAll
} yield ()
```
#### Metrics duplication
`KafkaMetricsRegistry` deduplicates metrics by default. It can be turned off by using a different factory method
accepting `allowDuplicates` parameter.
When using it in the default mode it's important to use different `client.id` values for different clients inside a
single VM, otherwise only one of them will be picked (order is not guaranteed).

## Setup

```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

libraryDependencies += "com.evolutiongaming" %% "skafka" % "11.5.0"
``` 
