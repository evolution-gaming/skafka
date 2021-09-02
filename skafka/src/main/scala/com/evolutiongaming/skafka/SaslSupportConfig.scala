package com.evolutiongaming.skafka

import cats.implicits._
import com.evolutiongaming.config.ConfigHelper.{ConfigOps, FromConf}
import com.evolutiongaming.skafka.ConfigHelpers.ConfigHelpersOps
import com.typesafe.config.{Config, ConfigException}
import org.apache.kafka.common.config.SaslConfigs

import java.nio.file.Path
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.util.{Failure, Success, Try}

final case class SaslSupportConfig(
  kerberosServiceName: Option[String]          = None,
  kerberosKinitCmd: Path                       = Path.of(SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD),
  kerberosTicketRenewWindowFactor: Double      = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,
  kerberosTicketRenewJitter: Double            = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER,
  kerberosMinTimeBeforeRelogin: FiniteDuration = SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN.millis,
  loginRefreshWindowFactor: Double             = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR,
  loginRefreshWindowJitter: Double             = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER,
  loginRefreshMinPeriod: FiniteDuration        = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS.seconds,
  loginRefreshBuffer: FiniteDuration           = SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS.seconds,
  mechanism: String                            = SaslConfigs.DEFAULT_SASL_MECHANISM,
  jaasConfig: Option[String]                   = None,
  clientCallbackHandlerClass: Option[Class[_]] = None,
  loginCallbackHandlerClass: Option[Class[_]]  = None,
  loginClass: Option[Class[_]]                 = None,
) {
  def bindings: Map[String, String] = {
    val bindings = Map[String, String](
      (SaslConfigs.SASL_KERBEROS_KINIT_CMD, kerberosKinitCmd.toFile.toString),
      (SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, kerberosTicketRenewWindowFactor.toString),
      (SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, kerberosTicketRenewJitter.toString),
      (SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, kerberosMinTimeBeforeRelogin.toMillis.toString),
      (SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, loginRefreshWindowFactor.toString),
      (SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, loginRefreshWindowJitter.toString),
      (SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, loginRefreshMinPeriod.toSeconds.toString),
      (SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, loginRefreshBuffer.toSeconds.toString),
      (SaslConfigs.SASL_MECHANISM, mechanism),
    )

    val optionalBindings = Map[String, Option[String]](
      (SaslConfigs.SASL_KERBEROS_SERVICE_NAME, kerberosServiceName),
      (SaslConfigs.SASL_JAAS_CONFIG, jaasConfig),
      (SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, clientCallbackHandlerClass.map(_.getName)),
      (SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, loginCallbackHandlerClass.map(_.getName)),
      (SaslConfigs.SASL_LOGIN_CLASS, loginClass.map(_.getName)),
    ).flattenOption

    bindings ++ optionalBindings
  }
}

object SaslSupportConfig {
  val Default: SaslSupportConfig = SaslSupportConfig()

  private implicit val FilePathFromConf: FromConf[Path] = FromConf[Path] { (conf, path) =>
    val str = conf.getString(path)
    Try(Path.of(str)) match {
      case Failure(exception) => throw new ConfigException.BadValue(conf.origin(), path, exception.getMessage)
      case Success(value)     => value
    }
  }

  private implicit val ClassFromConf: FromConf[Class[_]] = FromConf[Class[_]] { (conf, path) =>
    val className = conf.getString(path)
    Try(Class.forName(className)) match {
      case Failure(_)     => throw new ConfigException.BadValue(conf.origin(), path, s"Class '$className' doesn't exist")
      case Success(value) => value
    }
  }

  def apply(config: Config, default: => SaslSupportConfig): SaslSupportConfig =
    new SaslSupportConfig(
      kerberosServiceName = config.getOpt[String]("sasl-kerberos-service-name", "sasl.kerberos.service.name") orElse
        default.kerberosServiceName,
      kerberosKinitCmd = config.getOpt[Path]("sasl-kerberos-kinit-cmd", "sasl.kerberos.kinit.cmd") getOrElse
        default.kerberosKinitCmd,
      kerberosTicketRenewWindowFactor =
        config.getOpt[Double]("sasl-kerberos-ticket-renew-window-factor", "sasl.kerberos.ticket.renew.window.factor") getOrElse
          default.kerberosTicketRenewWindowFactor,
      kerberosTicketRenewJitter =
        config.getOpt[Double]("sasl-kerberos-ticket-renew-jitter", "sasl.kerberos.ticket.renew.jitter") getOrElse
          default.kerberosTicketRenewJitter,
      kerberosMinTimeBeforeRelogin =
        config.getMillis("sasl-kerberos-min-time-before-relogin", "sasl.kerberos.min.time.before.relogin.ms") getOrElse
          default.kerberosMinTimeBeforeRelogin,
      loginRefreshWindowFactor =
        config.getOpt[Double]("sasl-login-refresh-window-factor", "sasl.login.refresh.window.factor") getOrElse
          default.loginRefreshWindowFactor,
      loginRefreshWindowJitter =
        config.getOpt[Double]("sasl-login-refresh-window-jitter", "sasl.login.refresh.window.jitter") getOrElse
          default.loginRefreshWindowJitter,
      loginRefreshMinPeriod = config.getSeconds("sasl-login-refresh-min-period", "sasl.login.refresh.min.period.sec") getOrElse
        default.loginRefreshMinPeriod,
      loginRefreshBuffer = config.getSeconds("sasl-login-refresh-buffer", "sasl.login.refresh.buffer.sec") getOrElse
        default.loginRefreshBuffer,
      mechanism = config.getOpt[String]("sasl-mechanism", "sasl.mechanism") getOrElse
        default.mechanism,
      jaasConfig = config.getOpt[String]("sasl-jaas-config", "sasl.jaas.config") orElse
        default.jaasConfig,
      clientCallbackHandlerClass =
        config.getOpt[Class[_]]("sasl-client-callback-handler-class", "sasl.client.callback.handler.class") orElse
          default.clientCallbackHandlerClass,
      loginCallbackHandlerClass =
        config.getOpt[Class[_]]("sasl-login-callback-handler-class", "sasl.login.callback.handler.class") orElse
          default.loginCallbackHandlerClass,
      loginClass = config.getOpt[Class[_]]("sasl-login-class", "sasl.login.class") orElse
        default.loginClass,
    )
}
