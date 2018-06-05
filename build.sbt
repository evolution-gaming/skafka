import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/skafka")),
  startYear := Some(2018),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := "2.12.6",
  crossScalaVersions := Seq("2.12.6", "2.11.12"),
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Xfuture"),
  scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true)


lazy val skafka = (project
  in file(".")
  settings (name := "skafka")
  settings commonSettings
  aggregate(skafkaApi, skafkaImpl, logging, codahale, prometheus))

lazy val skafkaApi = (project
  in file("skafka-api")
  settings (name := "skafka-api")
  settings commonSettings
  settings (libraryDependencies ++= Seq(ScalaTest)))

lazy val skafkaImpl = (project
  in file("skafka-impl")
  settings (name := "skafka-impl")
  settings commonSettings
  dependsOn skafkaApi % "test->test;compile->compile"
  settings (libraryDependencies ++= Seq(
  Nel,
  ConfigTools,
  KafkaClients,
  Sequentially,
  ExecutorTools % Test)))

lazy val logging = (project
  in file("modules/logging")
  settings (name := "skafka-logging")
  settings commonSettings
  dependsOn skafkaApi
  settings (libraryDependencies ++= Seq(SafeActor)))

lazy val codahale = (project
  in file("modules/codahale")
  settings (name := "skafka-codahale")
  settings commonSettings
  dependsOn skafkaApi
  settings (libraryDependencies ++= Seq(MetricTools)))

lazy val prometheus = (project
  in file("modules/prometheus")
  settings (name := "skafka-prometheus")
  settings commonSettings
  dependsOn skafkaApi
  settings (libraryDependencies ++= Seq(Prometheus, ExecutorTools)))