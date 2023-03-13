package com.evolutiongaming.skafka

sealed trait ClientDnsLookup extends Product {
  def name: String
}

object ClientDnsLookup {
  val Values: Set[ClientDnsLookup] = Set(UseAllDnsIps, ResolveCanonicalBootstrapServersOnly)

  def useAllDnsIps: ClientDnsLookup = UseAllDnsIps

  def resolveCanonicalBootstrapServersOnly: ClientDnsLookup = ResolveCanonicalBootstrapServersOnly

  case object UseAllDnsIps extends ClientDnsLookup {
    def name: String = "use_all_dns_ips"
  }

  case object ResolveCanonicalBootstrapServersOnly extends ClientDnsLookup {
    def name: String = "resolve_canonical_bootstrap_servers_only"
  }
}