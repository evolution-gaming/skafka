import Dependencies._
import com.typesafe.tools.mima.core._

ThisBuild / versionScheme := Some("early-semver")
ThisBuild / evictionErrorLevel := Level.Warn
ThisBuild / versionPolicyIntention := Compatibility.BinaryCompatible

def crossSettings[T](scalaVersion: String, if3: List[T], if2: List[T]): List[T] =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((3, _))  => if3
    case Some((2, 13)) => if2
    case _             => Nil
  }

lazy val commonSettings = Seq(
  organization := "com.evolutiongaming",
  homepage := Some(uri("https://github.com/evolution-gaming/skafka")),
  startYear := Some(2018),
  organizationName := "Evolution",
  organizationHomepage := Some(uri("https://evolution.com")),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.13.18", "3.3.8"),
  licenses := Seq(("MIT", uri("https://opensource.org/licenses/MIT"))),
  Compile / doc / scalacOptions += "-no-link-warnings",
  scalacOptions ++= crossSettings(
    scalaVersion.value,
    if3 = List(
      "-release:17",
      "-Ykind-projector",
      "-language:implicitConversions",
      "-unchecked",
      "-feature",
      "-explain",
      "-explain-types",
      "-deprecation",
      "-Wunused:all",
    ),
    if2 = List(
      "-release:17",
      "-Xsource:3",
      "-deprecation",
    ),
  ),
  libraryDependencies ++= crossSettings(
    scalaVersion.value,
    if3 = Nil,
    if2 = List(compilerPlugin(`kind-projector`.cross(CrossVersion.full)))
  ),
  scalacOptsFailOnWarn := Some(false),
  publishTo := Some(Resolver.evolutionReleases),
  versionPolicyIgnored ++= Seq(
    // add libraries here that are known to be binary compatible, like:
    "com.evolutiongaming" %% "smetrics",
  ),
)

ThisBuild / mimaBinaryIssueFilters ++= Seq(
  // add mima check exceptions here, like:
//  ProblemFilters.exclude[ReversedMissingMethodProblem](
//    "com.evolutiongaming.skafka.consumer.Consumer.clientInstanceId"
//  ),
  ProblemFilters.exclude[ReversedMissingMethodProblem](
    "com.evolutiongaming.skafka.producer.Producer.sendOffsetsToTransaction"
  ),
)
ThisBuild / versionPolicyIgnored ++= Seq(
  // add libraries, that are known to be binary compatible, here, like:
//  "com.github.luben" %% "zstd-jni",
)

val alias: Seq[sbt.Def.Setting[?]] =
  addCommandAlias("fmt", "all scalafmtRepo") ++
    addCommandAlias("check", "all versionPolicyCheck Compile/doc scalafmtCheckRepo") ++
    addCommandAlias("build", "+all compile testFull")

lazy val root = project
  .in(file("."))
  .disablePlugins(MimaPlugin)
  .settings(name := "skafka-root")
  .settings(commonSettings)
  .settings(publish / skip := true)
  .settings(alias)
  .aggregate(
    skafka,
    `play-json`,
    metrics,
    `metrics-prometheus-v1`,
    tests,
  )

lazy val skafka = project
  .in(file("skafka"))
  .settings(commonSettings)
  .settings(
    name := "skafka",
    scalacOptions -= "-Ywarn-unused:params",
    libraryDependencies ++= Seq(
      Cats.core,
      CatsEffect.effect,
      CatsEffect.effectStd,
      `config-tools`,
      Kafka.clients,
      `future-helper`,
      `cats-helper`,
      Smetrics.smetrics,
      scalatest                % Test,
      Cats.laws                % Test,
      discipline               % Test,
      CatsEffect.effectTestKit % Test,
      `scala-java8-compat`,
      `collection-compat`
    )
  )

lazy val `play-json` = project
  .in(file("modules/play-json"))
  .settings(name := "skafka-play-json")
  .settings(commonSettings)
  .dependsOn(skafka)
  .settings(
    libraryDependencies ++= Seq(
      `play-json-jsoniter`,
      scalatest % Test,
    )
  )

lazy val metrics = project
  .in(file("modules/metrics"))
  .settings(name := "skafka-metrics")
  .settings(commonSettings)
  .dependsOn(skafka)
  .settings(
    libraryDependencies ++= Seq(
      Smetrics.`smetrics-prometheus`,
    )
  )

lazy val `metrics-prometheus-v1` = project
  .in(file("modules/metrics_prometheus_v1"))
  .settings(commonSettings)
  .dependsOn(skafka)
  .settings(
    libraryDependencies ++= Seq(
      Smetrics.`smetrics-prometheus-v1`,
      scalatest                % Test,
      CatsEffect.effectTestKit % Test,
    )
  )
  .settings(name := "skafka-metrics-prometheus-v1")
  .settings(organization := "com.evolution")

lazy val tests = project
  .in(file("tests"))
  .settings(name := "skafka-tests")
  .settings(commonSettings)
  .settings(
    Seq(
      publish / skip := true,
      Test / fork := true,
      Test / parallelExecution := false,
    )
  )
  .dependsOn(skafka % "compile->compile;test->test")
  .settings(
    libraryDependencies ++= Seq(
      `testcontainers-kafka`   % Test,
      Slf4j.api                % Test,
      Slf4j.`log4j-over-slf4j` % Test,
      Logback.core             % Test,
      Logback.classic          % Test,
      scalatest                % Test
    )
  )
