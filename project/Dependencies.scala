import sbt._

object Dependencies {

  val `config-tools`         = "com.evolutiongaming"    %% "config-tools"               % "1.0.5"
  val `future-helper`        = "com.evolutiongaming"    %% "future-helper"              % "1.0.7"
  val `cats-helper`          = "com.evolutiongaming"    %% "cats-helper"                % "3.11.0"
  val `testcontainers-kafka` = "com.dimafeng"           %% "testcontainers-scala-kafka" % "0.41.0"
  val `play-json-jsoniter`   = "com.evolution"          %% "play-json-jsoniter"         % "1.1.1"
  val `scala-java8-compat`   = "org.scala-lang.modules" %% "scala-java8-compat"         % "1.0.2"
  val `collection-compat`    = "org.scala-lang.modules" %% "scala-collection-compat"    % "2.11.0"
  val scalatest              = "org.scalatest"          %% "scalatest"                  % "3.2.17"
  val `kind-projector`       = "org.typelevel"           % "kind-projector"             % "0.13.2"
  val discipline             = "org.typelevel"          %% "discipline-scalatest"       % "2.2.0"

  object Kafka {
    private val version = "3.4.0"
    val clients         = "org.apache.kafka" % "kafka-clients" % version
  }

  object Logback {
    private val version = "1.4.11"
    val core            = "ch.qos.logback" % "logback-core"    % version
    val classic         = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version    = "2.0.9"
    val api                = "org.slf4j" % "slf4j-api"        % version
    val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Cats {
    private val version = "2.12.0"
    val core            = "org.typelevel" %% "cats-core" % version
    val laws            = "org.typelevel" %% "cats-laws" % version
  }

  object CatsEffect {
    private val version = "3.5.4"
    val effect          = "org.typelevel" %% "cats-effect"         % version
    val effectStd       = "org.typelevel" %% "cats-effect-std"     % version
    val effectTestKit   = "org.typelevel" %% "cats-effect-testkit" % version
  }

  object Smetrics {
    private val version       = "2.2.0"
    val smetrics              = "com.evolutiongaming" %% "smetrics"            % version
    val `smetrics-prometheus` = "com.evolutiongaming" %% "smetrics-prometheus" % version
  }
}
