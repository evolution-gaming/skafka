# Skafka
[![Build Status](https://github.com/evolution-gaming/skafka/workflows/CI/badge.svg)](https://github.com/evolution-gaming/skafka/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/evolution-gaming/skafka/badge.svg)](https://coveralls.io/r/evolution-gaming/skafka)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/faac7c4d0b924320b60ce9eefc360b12)](https://www.codacy.com/app/evolution-gaming/skafka?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/skafka&amp;utm_campaign=Badge_Grade)
[![version](https://api.bintray.com/packages/evolutiongaming/maven/skafka/images/download.svg)](https://bintray.com/evolutiongaming/maven/skafka/_latestVersion)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

Scala wrapper for [kafka-clients v2.5.0](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/2.5.0)

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
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "skafka" % "10.0.0"
``` 
