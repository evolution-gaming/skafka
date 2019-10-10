import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/skafka")),
  startYear := Some(2018),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq(/*"2.13.0", */"2.12.10"),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  scalacOptions in(Compile, doc) += "-no-link-warnings",
  libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.binary))


lazy val root = (project
  in file(".")
  settings (name := "skafka")
  settings commonSettings
  settings (skip in publish := true)
  aggregate(skafka, `play-json`, tests))

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
      `config-tools`,
      Kafka.`kafka-clients`,
      `future-helper`,
      `cats-helper`,
      smetrics,
      scalatest % Test,
      `scala-java8-compat`)))

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
      Slf4j.api % Test,
      Slf4j.`log4j-over-slf4j` % Test,
      Logback.core % Test,
      Logback.classic % Test,
      scalatest % Test)))