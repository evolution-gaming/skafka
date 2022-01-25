import sbt._

object Dependencies {

  val `executor-tools`     = "com.evolutiongaming"    %% "executor-tools"          % "1.0.2"
  val `config-tools`       = "com.evolutiongaming"    %% "config-tools"            % "1.0.4"
  val `future-helper`      = "com.evolutiongaming"    %% "future-helper"           % "1.0.6"
  val `cats-helper`        = "com.evolutiongaming"    %% "cats-helper"             % "3.0.1"
  val `kafka-launcher`     = "com.evolutiongaming"    %% "kafka-launcher"          % "0.0.11"
  val `play-json-jsoniter` = "com.evolutiongaming"    %% "play-json-jsoniter"      % "0.9.3"
  val `scala-java8-compat` = "org.scala-lang.modules" %% "scala-java8-compat"      % "1.0.0"
  val `collection-compat`  = "org.scala-lang.modules" %% "scala-collection-compat" % "2.5.0"
  val scalatest            = "org.scalatest"          %% "scalatest"               % "3.2.10"
  val smetrics             = "com.evolutiongaming"    %% "smetrics"                % "1.0.1"
  val `kind-projector`     = "org.typelevel"           % "kind-projector"          % "0.13.2"
  val discipline           = "org.typelevel"          %% "discipline-scalatest"    % "2.1.5"

  object Kafka {
    private val version = "2.7.2"
    val kafka           = "org.apache.kafka" %% "kafka"         % version
    val clients         = "org.apache.kafka"  % "kafka-clients" % version
  }

  object Logback {
    private val version = "1.2.7"
    val core            = "ch.qos.logback" % "logback-core"    % version
    val classic         = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version    = "1.7.35"
    val api                = "org.slf4j" % "slf4j-api"        % version
    val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Cats {
    private val version = "2.7.0"
    val core            = "org.typelevel" %% "cats-core" % version
    val laws            = "org.typelevel" %% "cats-laws" % version
  }

  object CatsEffect {
    private val version = "3.3.4"
    val effect          = "org.typelevel" %% "cats-effect"     % version
    val effectStd       = "org.typelevel" %% "cats-effect-std" % version
  }
}
