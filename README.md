# Skafka [![Build Status](https://travis-ci.org/evolution-gaming/skafka.svg)](https://travis-ci.org/evolution-gaming/skafka) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/skafka/badge.svg)](https://coveralls.io/r/evolution-gaming/skafka) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/faac7c4d0b924320b60ce9eefc360b12)](https://www.codacy.com/app/evolution-gaming/skafka?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/skafka&amp;utm_campaign=Badge_Grade) [![version](https://api.bintray.com/packages/evolutiongaming/maven/skafka/images/download.svg)](https://bintray.com/evolutiongaming/maven/skafka/_latestVersion) [![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

Scala wrapper for [kafka-clients v1.1.1](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/1.1.1)

It provides non-blocking and null-less apis:
* [Producer](src/main/scala/com/evolutiongaming/skafka/producer/Producer.scala) 
* [Consumer](src/main/scala/com/evolutiongaming/skafka/consumer/Consumer.scala)  


## Producer usage example

```scala
val ecBlocking: ExecutionContext = ExecutionContext.global // do not use `global` in production
val config = ProducerConfig.Default
val producer = Producer(config, ecBlocking)
val record = ProducerRecord(topic = "topic", key = "key", value = "value")
val metadata: Future[RecordMetadata] = producer.send(record)
```

## Consumer usage example

```scala
val ecBlocking: ExecutionContext = ExecutionContext.global // do not use `global` in production
val config = ConsumerConfig.Default
val consumer = Consumer[String, String](config, ecBlocking)
consumer.subscribe(Nel("topic"), None)
val future: Future[ConsumerRecords[String, String]] = consumer.poll(100.millis)
```

## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "skafka" % "2.1.0"

libraryDependencies += "com.evolutiongaming" %% "skafka-logging" % "2.1.0"

libraryDependencies += "com.evolutiongaming" %% "skafka-codahale" % "2.1.0"

libraryDependencies += "com.evolutiongaming" %% "skafka-prometheus" % "2.1.0"

libraryDependencies += "com.evolutiongaming" %% "skafka-play-json" % "2.1.0"
``` 
