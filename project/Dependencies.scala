import sbt._

object Dependencies {
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.5" % Test
  val `kafka-clients` = "org.apache.kafka" % "kafka-clients" % "1.1.1"
  val `executor-tools` = "com.evolutiongaming" %% "executor-tools" % "1.0.1"
  val sequentially = "com.evolutiongaming" %% "sequentially" % "1.0.12"
  val nel = "com.evolutiongaming" %% "nel" % "1.3.1"
  val `config-tools` = "com.evolutiongaming" %% "config-tools" % "1.0.3"
  val `metric-tools` = "com.evolutiongaming" %% "metric-tools" % "1.1"
  val `safe-actor` = "com.evolutiongaming" %% "safe-actor" % "1.7"
  val Prometheus = "io.prometheus" % "simpleclient" % "0.5.0"
  val `future-helper` = "com.evolutiongaming" %% "future-helper" % "1.0.3"
  val `play-json` = "com.typesafe.play" %% "play-json" % "2.6.10"
  val `kafka-launcher` = "com.evolutiongaming" %% "kafka-launcher" % "0.0.1" % Test

  object Akka {
    private val version = "2.5.16"
    val testkit = "com.typesafe.akka" %% "akka-testkit" % version % Test
    val slf4j = "com.typesafe.akka" %% "akka-slf4j" % version % Test
  }

  object Logback {
    private val version = "1.2.3"
    val core = "ch.qos.logback" % "logback-core" % version
    val classic = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version = "1.7.25"
    val api = "org.slf4j" % "slf4j-api" % version
    val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }
}
