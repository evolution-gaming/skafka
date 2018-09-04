import sbt._

object Dependencies {
  lazy val scalatest = "org.scalatest" %% "scalatest" % "3.0.5" % Test
  lazy val `kafka-clients` = "org.apache.kafka" % "kafka-clients" % "1.1.1"
  lazy val `executor-tools` = "com.evolutiongaming" %% "executor-tools" % "1.0.1"
  lazy val Sequentially = "com.evolutiongaming" %% "sequentially" % "1.0.12"
  lazy val Nel = "com.evolutiongaming" %% "nel" % "1.3.1"
  lazy val `config-tools` = "com.evolutiongaming" %% "config-tools" % "1.0.2"
  lazy val `metric-tools` = "com.evolutiongaming" %% "metric-tools" % "1.1"
  lazy val `safe-actor` = "com.evolutiongaming" %% "safe-actor" % "1.7"
  lazy val Prometheus = "io.prometheus" % "simpleclient" % "0.5.0"
  lazy val `future-helper` = "com.evolutiongaming" %% "future-helper" % "1.0.3"
  lazy val `play-json` = "com.typesafe.play" %% "play-json" % "2.6.10"
  lazy val `kafka-launcher` = "com.evolutiongaming" %% "kafka-launcher" % "0.0.1" % Test

  object Akka {
    private val version = "2.5.15"
    lazy val testkit = "com.typesafe.akka" %% "akka-testkit" % version % Test
    lazy val slf4j = "com.typesafe.akka" %% "akka-slf4j" % version % Test
  }

  object Logback {
    private val version = "1.2.3"
    lazy val core = "ch.qos.logback" % "logback-core" % version
    lazy val classic = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version = "1.7.25"
    lazy val api = "org.slf4j" % "slf4j-api" % version
    lazy val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }
}
