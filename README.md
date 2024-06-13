# Skafka
[![Build Status](https://github.com/evolution-gaming/skafka/workflows/CI/badge.svg)](https://github.com/evolution-gaming/skafka/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/skafka/badge.svg?branch=master)](https://coveralls.io/github/evolution-gaming/skafka?branch=master)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/2373830b1e624ed39d27a644dca63d17)](https://app.codacy.com/gh/evolution-gaming/skafka/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolutiongaming&a=skafka_2.13&repos=public)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

Scala wrapper for [kafka-clients v3.4.0](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/3.4.0)

## Motivation

Kafka provides an official Java client out of the box, which could be used from
Scala code without any additional modifications.

The main disadvantage of using an official client directly is that it implies
a very specific threading model to the application. I.e. the consumer is not
thread safe and also expects a rebalance listener to do the operations in the
same thread.

This makes wrapping a client with [Cats Effect](https://typelevel.org/cats-effect/)
classes a bit more complicated than just calling `IO { consumer.poll() }` unless
this is the only call, which is expected to be used.

Skafka does exactly that: a very thin wrapper over official Kafka client to
provide a ready-made Cats Effect API and handle some corner cases concerning
[ConsumerRebalanceListener](https://kafka.apache.org/34/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html) calls.

Comparing to more full-featured libraries such as
[FS2 Kafka](https://fd4s.github.io/fs2-kafka), it might be a little bit more
reliable, because there is little code/logic to hide the accidenital bugs in.

To summarize:
1. If it suits your goals (i.e. you only ever need to do `consumer.poll()`
without acting on rebalance etc.) then using an official Kafka client directly,
optionally, wrapping all the calls with `cats.effect.IO`, is a totally fine idea.
2. If more complicated integration to Cats Effect is required, i.e.
_ConsumerRebalanceListener_ is going to be used then consider using _Skafka_.
3. If streaming with [FS2](https://fs2.io) is required or any other features
the library provides then _FS2 Kafka_ could be a good choice. Note, that it is
less trivial then _Skafka_ and may contain more bugs on top of the official
Kafka client.

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

libraryDependencies += "com.evolutiongaming" %% "skafka" % "15.0.0"
``` 

## Notes

While _Skafka_ provides an ability to use `ConsumerRebalanceListener`
functionality, not all of the method calls are supported.

See the following PRs for more details:
https://github.com/evolution-gaming/skafka/pull/150
https://github.com/evolution-gaming/skafka/pull/122

To our latest knowledge neither `FS2 Kafka` supports all of the
methods / functionality.