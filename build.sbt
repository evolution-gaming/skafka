import Dependencies._
import com.typesafe.tools.mima.core._

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / evictionErrorLevel := Level.Warn

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(new URL("https://github.com/evolution-gaming/skafka")),
  startYear := Some(2018),
  organizationName := "Evolution",
  organizationHomepage := Some(url("https://evolution.com")),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.13.8", "2.12.15"),
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  releaseCrossBuild := true,
  Compile / doc / scalacOptions += "-no-link-warnings",
  libraryDependencies += compilerPlugin(`kind-projector` cross CrossVersion.full),
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
  aggregate(skafka, `play-json`, metrics, tests))

lazy val skafka = (project in file("skafka")
  disablePlugins (MimaPlugin)
  settings commonSettings
  settings(                                                                          
    name := "skafka",
    scalacOptions -= "-Ywarn-unused:params",
    libraryDependencies ++= Seq(
      Cats.core,
      CatsEffect.effect,
      `config-tools`,
      Kafka.clients,
      `future-helper`,
      `cats-helper`,
      Smetrics.smetrics,
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

lazy val metrics = (project in file("modules/metrics")
  settings (name := "skafka-metrics")
  disablePlugins (MimaPlugin)
  settings commonSettings
  dependsOn skafka
  settings (libraryDependencies ++= Seq(Smetrics.`smetrics-prometheus`)))

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