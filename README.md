# Skafka [![Build Status](https://travis-ci.org/evolution-gaming/skafka.svg)](https://travis-ci.org/evolution-gaming/skafka) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/skafka/badge.svg)](https://coveralls.io/r/evolution-gaming/skafka) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/faac7c4d0b924320b60ce9eefc360b12)](https://www.codacy.com/app/evolution-gaming/skafka?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/skafka&amp;utm_campaign=Badge_Grade) [![version](https://api.bintray.com/packages/evolutiongaming/maven/skafka/images/download.svg)](https://bintray.com/evolutiongaming/maven/skafka/_latestVersion) [![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

Scala wrapper for [kafka-clients v2.2.1](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/2.1.0)

It provides non-blocking and null-less apis:
* [Producer](skafka/src/main/scala/com/evolutiongaming/skafka/producer/Producer.scala) 
* [Consumer](skafka/src/main/scala/com/evolutiongaming/skafka/consumer/Consumer.scala)  


## Producer usage example

```scala
val producer = Producer.of[IO](config, ecBlocking)
val metadata: IO[RecordMetadata] = Producer.of[IO](config, ecBlocking).use { producer =>
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

libraryDependencies += "com.evolutiongaming" %% "skafka" % "4.0.4"

libraryDependencies += "com.evolutiongaming" %% "skafka-logging" % "4.0.4"

libraryDependencies += "com.evolutiongaming" %% "skafka-prometheus" % "4.0.4"

libraryDependencies += "com.evolutiongaming" %% "skafka-play-json" % "4.0.4"
``` 
