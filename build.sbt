import Dependencies._
import com.typesafe.tools.mima.core._

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("https://github.com/evolution-gaming/skafka")),
  startYear := Some(2018),
  organizationName := "Evolution",
  organizationHomepage := Some(url("https://evolution.com")),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.13.3", "2.12.14"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  Compile / doc / scalacOptions += "-no-link-warnings",
  libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.binary),
  scalacOptsFailOnWarn := Some(false),
  publishTo := Some(Resolver.evolutionReleases),
  // KeyRanks.Invisible to suppress sbt warning about key not being used in root/tests where MiMa plugin is disabled
  mimaPreviousArtifacts.withRank(KeyRanks.Invisible) := {
    val versions = List(
      "11.0.0",
    )
    versions.map(organization.value %% moduleName.value % _).toSet
  },
  mimaBinaryIssueFilters ++= Seq(
    ProblemFilters.exclude[ReversedMissingMethodProblem]("com.evolutiongaming.skafka.consumer.Consumer.subscribe"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("com.evolutiongaming.skafka.Converters#MapJOps.asScalaMap$extension")
  )
)


lazy val root = (project in file(".")
  disablePlugins (MimaPlugin)
  settings (name := "skafka")
  settings commonSettings
  settings (publish / skip := true)
  aggregate(skafka, `play-json`, tests))

lazy val skafka = (project in file("skafka")
  disablePlugins (MimaPlugin)
  settings commonSettings
  settings(                                                                          
    name := "skafka",
    scalacOptions -= "-Ywarn-unused:params",
    libraryDependencies ++= Seq(
      Cats.core,
      Cats.effect,
      `config-tools`,
      Kafka.clients,
      `future-helper`,
      `cats-helper`,
      smetrics,
      scalatest % Test,
      Cats.laws % Test,
      discipline % Test,
      `scala-java8-compat`,
      `collection-compat`)))

lazy val `play-json` = (project in file("modules/play-json")
  settings (name := "skafka-play-json")
  settings commonSettings
  dependsOn skafka
  settings (libraryDependencies ++= Seq(Dependencies.`play-json-jsoniter`, scalatest % Test)))

lazy val tests = (project in file("tests")
  disablePlugins (MimaPlugin)
  settings (name := "skafka-tests")
  settings commonSettings
  settings Seq(
    publish / skip := true,
    Test / fork := true,
    Test / parallelExecution := false)
    dependsOn skafka % "compile->compile;test->test"
    settings (libraryDependencies ++= Seq(
      Kafka.kafka % Test,
      `kafka-launcher` % Test,
      Slf4j.api % Test,
      Slf4j.`log4j-over-slf4j` % Test,
      Logback.core % Test,
      Logback.classic % Test,
      scalatest % Test)))