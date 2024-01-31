# Skafka
[![Build Status](https://github.com/evolution-gaming/skafka/workflows/CI/badge.svg)](https://github.com/evolution-gaming/skafka/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/skafka/badge.svg?branch=master)](https://coveralls.io/github/evolution-gaming/skafka?branch=master)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/2373830b1e624ed39d27a644dca63d17)](https://app.codacy.com/gh/evolution-gaming/skafka/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolutiongaming&a=skafka_2.13&repos=public)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

Scala wrapper for [kafka-clients v3.4.0](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/3.4.0)

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

## Setup

```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

libraryDependencies += "com.evolutiongaming" %% "skafka" % "15.0.0"
``` 
