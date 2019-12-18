import sbt._

object Dependencies {

  val `executor-tools`     = "com.evolutiongaming"    %% "executor-tools"          % "1.0.2"
  val `config-tools`       = "com.evolutiongaming"    %% "config-tools"            % "1.0.4"
  val `future-helper`      = "com.evolutiongaming"    %% "future-helper"           % "1.0.6"
  val `cats-helper`        = "com.evolutiongaming"    %% "cats-helper"             % "1.4.0"
  val `kafka-launcher`     = "com.evolutiongaming"    %% "kafka-launcher"          % "0.0.9"
  val `play-json`          = "com.typesafe.play"      %% "play-json"               % "2.8.1"
  val `scala-java8-compat` = "org.scala-lang.modules" %% "scala-java8-compat"      % "0.9.0"
  val `collection-compat`  = "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.3"
  val scalatest            = "org.scalatest"          %% "scalatest"               % "3.1.0"
  val smetrics             = "com.evolutiongaming"    %% "smetrics"                % "0.0.8"
  val `kind-projector`     = "org.typelevel"           % "kind-projector"          % "0.10.3"

  object Kafka {
    private val version = "2.4.0"
    val kafka   = "org.apache.kafka" %% "kafka"         % version
    val clients = "org.apache.kafka" %  "kafka-clients" % version
  }

  object Logback {
    private val version = "1.2.3"
    val core    = "ch.qos.logback" % "logback-core"    % version
    val classic = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version = "1.7.30"
    val api                = "org.slf4j" % "slf4j-api"        % version
    val `log4j-over-slf4j` = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Cats {
    private val version = "2.0.0"
    val core   = "org.typelevel" %% "cats-core"   % version
    val kernel = "org.typelevel" %% "cats-kernel" % version
    val macros = "org.typelevel" %% "cats-macros" % version
    val effect = "org.typelevel" %% "cats-effect" % "2.0.0"
  }
}
