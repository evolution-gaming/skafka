import sbt._

object Dependencies {
  lazy val scalatest = "org.scalatest" %% "scalatest" % "3.0.5" % Test
  lazy val `kafka-clients` = "org.apache.kafka" % "kafka-clients" % "1.1.1"
  lazy val `executor-tools` = "com.evolutiongaming" %% "executor-tools" % "1.0.0"
  lazy val Sequentially = "com.evolutiongaming" %% "sequentially" % "1.0.9"
  lazy val Nel = "com.evolutiongaming" %% "nel" % "1.2"
  lazy val `config-tools` = "com.evolutiongaming" %% "config-tools" % "1.0.2"
  lazy val `metric-tools` = "com.evolutiongaming" %% "metric-tools" % "1.1"
  lazy val `safe-actor` = "com.evolutiongaming" %% "safe-actor" % "1.6"
  lazy val Prometheus = "io.prometheus" % "simpleclient" % "0.4.0"
  lazy val `future-helper` = "com.evolutiongaming" %% "future-helper" % "1.0.2"
  lazy val `play-json` = "com.typesafe.play" %% "play-json" % "2.6.9"
}
