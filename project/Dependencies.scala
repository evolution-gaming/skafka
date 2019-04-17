import sbt._

object Dependencies {

  val scalatest = "org.scalatest" %% "scalatest" % "3.0.7" % Test

  val `executor-tools` = "com.evolutiongaming" %% "executor-tools" % "1.0.1"

  val sequentially = "com.evolutiongaming" %% "sequentially" % "1.0.14"

  val nel = "com.evolutiongaming" %% "nel" % "1.3.3"

  val `config-tools` = "com.evolutiongaming" %% "config-tools" % "1.0.3"

  val `metric-tools` = "com.evolutiongaming" %% "metric-tools" % "1.1"

  val `safe-actor` = "com.evolutiongaming" %% "safe-actor" % "2.0.3"

  val prometheus = "io.prometheus" % "simpleclient" % "0.6.0"

  val `future-helper` = "com.evolutiongaming" %% "future-helper" % "1.0.4"

  val `play-json` = "com.typesafe.play" %% "play-json" % "2.7.3"
  
  val `kafka-launcher` = "com.evolutiongaming" %% "kafka-launcher" % "0.0.6"

  val `scala-java8-compat` = "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0"

  object Kafka {
    private val version = "2.1.1"
    val kafka           = "org.apache.kafka" %% "kafka" % version
    val `kafka-clients` = "org.apache.kafka" %  "kafka-clients" % version
  }

  object Akka {
    private val version = "2.5.22"
    val actor   = "com.typesafe.akka" %% "akka-actor" % version
    val stream  = "com.typesafe.akka" %% "akka-stream" % version
    val testkit = "com.typesafe.akka" %% "akka-testkit" % version
    val slf4j   = "com.typesafe.akka" %% "akka-slf4j" % version
  }

  object Logback {
    private val version = "1.2.3"
    val core    = "ch.qos.logback" % "logback-core" % version
    val classic = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version = "1.7.26"
    val api                = "org.slf4j" % "slf4j-api" % version
    val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }
}
