import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/skafka")),
  startYear := Some(2018),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := crossScalaVersions.value.last,
  crossScalaVersions := Seq("2.12.8"),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  scalacOptions in(Compile, doc) += "-no-link-warnings",
  libraryDependencies += compilerPlugin("org.typelevel" % "kind-projector" % "0.10.3" cross CrossVersion.binary))


lazy val root = (project
  in file(".")
  settings (name := "skafka")
  settings commonSettings
  settings (skip in publish := true)
  aggregate(skafka, prometheus, `play-json`, tests))

lazy val skafka = (project
  in file("skafka")
  settings commonSettings
  settings(                                                                          
    name := "skafka",
    scalacOptions -= "-Ywarn-unused:params",
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.kernel,
      Cats.macros,
      Cats.effect,
      Akka.actor,
      Akka.stream,
      nel,
      `config-tools`,
      Kafka.`kafka-clients`,
      `future-helper`,
      `cats-effect`,
      `cats-helper`,
      scalatest % Test,
      `scala-java8-compat`)))

lazy val prometheus = (project
  in file("modules/prometheus")
  settings (name := "skafka-prometheus")
  settings commonSettings
  dependsOn skafka % "compile->compile;test->test"
  settings (libraryDependencies ++= Seq(Dependencies.prometheus, `executor-tools`, scalatest % Test)))

lazy val `play-json` = (project
  in file("modules/play-json")
  settings (name := "skafka-play-json")
  settings commonSettings
  dependsOn skafka
  settings (libraryDependencies ++= Seq(Dependencies.`play-json`, scalatest % Test)))

lazy val tests = (project in file("tests")
  settings (name := "skafka-tests")
  settings commonSettings
  settings Seq(
    skip in publish := true,
    Test / fork := true,
    Test / parallelExecution := false)
    dependsOn skafka % "compile->compile;test->test"
    settings (libraryDependencies ++= Seq(
      Kafka.kafka % Test,
      `kafka-launcher` % Test,
      Akka.testkit % Test,
      Akka.slf4j % Test,
      Slf4j.api % Test,
      Slf4j.`log4j-over-slf4j` % Test,
      Logback.core % Test,
      Logback.classic % Test,
      scalatest % Test)))