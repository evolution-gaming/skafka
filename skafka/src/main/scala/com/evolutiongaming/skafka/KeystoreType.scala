package com.evolutiongaming.skafka

sealed trait KeystoreType extends Product {
  def name: String
}

object KeystoreType {

  val Values: Set[KeystoreType] = Set(JKS, JCEKS, PKCS12, PKCS11, DKS, WindowsMY, BKS)

  case object JKS extends KeystoreType { def name = "JKS" }
  case object JCEKS extends KeystoreType { def name = "JCEKS" }
  case object PKCS12 extends KeystoreType { def name = "PKCS12" }
  case object PKCS11 extends KeystoreType { def name = "PKCS11" }
  case object DKS extends KeystoreType { def name = "DKS" }
  case object WindowsMY extends KeystoreType { def name = "Windows-MY" }
  case object BKS extends KeystoreType { def name = "BKS" }
}
