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
import cats.data.{NonEmptySet => Nes}

val consumer = Consumer.of[IO, String, String](config, ecBlocking)
val records: IO[ConsumerRecords[String, String]] = consumer.use { consumer => 
  for {
    _       <- consumer.subscribe(Nes("topic"))
    records <- consumer.poll(100.millis)
  } yield records 
}
```

## Handling consumer group rebalance
It's possible to provide a callback for a consumer group rebalance event, which can be useful if you want to do some computations,
save the state, commit some offsets (or do anything with the consumer whenever partition assignment changes).  
This can be done by providing an implementation of `RebalanceListener1` (or a more convenient version - `RebalanceListener1WithConsumer`) 
which requires you to return a `RebalanceCallback` structure which describes what actions should be performed in a certain situation. 
It allows you to use some of consumer's methods as well as a way to run an arbitrary computation. 
### Note on long-running computations in a rebalance callback 
Please note that all the actions are executed on the consumer `poll` thread which means that running heavy or 
long-running computations is discouraged. This is due to to the following reasons:
- if executing callback takes too long (longer than Kafka consumer `max.poll.interval.ms` setting), the consumer will be assumed
'failed' and the group will rebalance once again. The default value is 300000 (5 minutes). You can see the official documentation [here](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_max.poll.interval.ms)
- since the callback is executed by the means of providing an instance of `ToTry` (which runs the computation synchronously in the Java callback), it dictates the timeout for the callback computation to complete. 
The current default implementation for `cats.effect.IO` is 1 minute (see `ToTry#ioToTry`)

### What you can currently do:
- lift a pure value via `RebalanceCallback.pure(a)`. There's also an instance of `cats.Monad` for `RebalanceCallback` which you can use via syntax extensions or direct summoning
- raise an error which should be an instance of `Throwable`. If your effect `F[_]` has an instance of `MonadError[F, Throwable]` in scope, then an instance of `MonadError[RebalanceCallback[F, *], Throwable]` will be derived, so you can use it to `raiseError` via syntax extensions or direct summoning
- use consumer methods, for example `RebalanceCallback.commit` or `RebalanceCallback.seek`
  (see `RebalanceCallbackApi` to discover allowed consumer methods)
- lift any arbitrary computation in the `F[_]` effect via `RebalanceCallback.lift(...)`
- handle occuring errors in the `F[_]` effect via `callback.handleErroWith(...)` or using `MonadError` instance, described above  

These operations can be composed due to the presence of `map`/`flatMap` methods as well as the presence of `cats.Monad` instance.
### Example
```scala
import cats.data.{NonEmptySet => Nes}

class SaveOffsetsOnRebalance[F[_]: Applicative] extends RebalanceListener1WithConsumer[F] {

    // import is needed to use `fa.lift` syntax where
    // `fa: F[A]`
    // `fa.lift: RebalanceCallback[F, A]`
    import RebalanceCallback.syntax._

    def onPartitionsAssigned(partitions: Nes[TopicPartition]) =
      for {
        // read the offsets from an external store using some custom code not described here
        offsets <- readOffsetsFromExternalStore[F](partitions).lift
        a       <- offsets.toList.foldMapM { case (partition, offset) => consumer.seek(partition, offset) }
      } yield a

    def onPartitionsRevoked(partitions: Nes[TopicPartition]) =
      for {
        positions <- partitions.foldM(Map.empty[TopicPartition, Offset]) {
          case (offsets, partition) =>
            for {
              position <- consumer.position(partition)
            } yield offsets + (partition -> position)
        }
        // save the offsets in an external store using some custom code not described here
        a <- saveOffsetsInExternalStore[F](positions).lift
      } yield a

    // do not need to save the offsets since these partitions are probably owned by other consumers already
    def onPartitionsLost(partitions: Nes[TopicPartition]) = RebalanceCallback.empty

    private def readOffsetsFromExternalStore[F[_]: Applicative](partitions: Nes[TopicPartition]): F[Map[TopicPartition, Offset]] = ???
    private def saveOffsetsInExternalStore[F[_]: Applicative](offsets: Map[TopicPartition, Offset]): F[Unit] = ???
  }

val consumer = Consumer.of[IO, String, String](config, ecBlocking)
val listener = new SaveOffsetsOnRebalance[IO]
val records: IO[ConsumerRecords[String, String]] = consumer.use { consumer =>
  for {
    _       <- consumer.subscribe(Nes("topic"), listener)
    records <- consumer.poll(100.millis)
  } yield records
}
```

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