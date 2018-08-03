import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/skafka")),
  startYear := Some(2018),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := crossScalaVersions.value.last,
  crossScalaVersions := Seq("2.11.12", "2.12.6"),
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
//    "-Xfatal-warnings",
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
  settings (skip in publish := true)
  aggregate(api, impl, logging, codahale, prometheus, `play-json`))

lazy val api = (project
  in file("skafka-api")
  settings (name := "skafka-api")
  settings commonSettings
  settings (libraryDependencies ++= Seq(`future-helper`, scalatest)))

lazy val impl = (project
  in file("skafka-impl")
  settings (name := "skafka-impl")
  settings commonSettings
  dependsOn api % "test->test;compile->compile"
  settings (libraryDependencies ++= Seq(
  Nel,
  `config-tools`,
  `kafka-clients`,
  Sequentially,
  `executor-tools` % Test)))

lazy val logging = (project
  in file("modules/logging")
  settings (name := "skafka-logging")
  settings commonSettings
  dependsOn api
  settings (libraryDependencies ++= Seq(`safe-actor`)))

lazy val codahale = (project
  in file("modules/codahale")
  settings (name := "skafka-codahale")
  settings commonSettings
  dependsOn api
  settings (libraryDependencies ++= Seq(`metric-tools`)))

lazy val prometheus = (project
  in file("modules/prometheus")
  settings (name := "skafka-prometheus")
  settings commonSettings
  dependsOn api
  settings (libraryDependencies ++= Seq(Prometheus, `executor-tools`)))

lazy val `play-json` = (project
  in file("modules/play-json")
  settings (name := "skafka-play-json")
  settings commonSettings
  dependsOn api
  settings (libraryDependencies ++= Seq(Dependencies.`play-json`, scalatest)))