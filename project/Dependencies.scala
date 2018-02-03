import sbt._

object Dependencies {
  lazy val ScalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % Test
  lazy val KafkaClients = "org.apache.kafka" % "kafka-clients" % "1.0.0"
//  lazy val KafkaClients_010 = "org.apache.kafka" % "kafka-clients" % "0.10.2.1"
  lazy val ExecutorTools = "com.evolutiongaming" %% "executor-tools" % "1.0.0"
}
