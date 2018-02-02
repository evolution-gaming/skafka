import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("http://github.com/evolution-gaming/skafka")),
  startYear := Some(2018),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  scalaVersion := "2.12.4",
  crossScalaVersions := Seq("2.12.4", "2.11.12"),
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
  aggregate(skafkaApi, skafkaImpl))

lazy val skafkaApi = (project
  in file("skafka-api")
  settings (name := "skafka-api")
  settings commonSettings
  settings (libraryDependencies ++= Seq(ScalaTest)))

lazy val skafkaImpl = (project
  in file("skafka-impl")
  settings (name := "skafka-impl")
  settings commonSettings
  dependsOn skafkaApi % "test->test;compile->compile")