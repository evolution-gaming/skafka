package com.evolutiongaming.skafka.circe

import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.skafka.ToBytes
import io.circe.Encoder
import cats.syntax.contravariant.toContravariantOps
import io.circe.syntax.EncoderOps

object ToBytesFromEncoder {
  def apply[F[_]: FromTry, A: Encoder]: ToBytes[F, A] = ToBytes.stringToBytes[F].contramap(_.asJson.noSpaces)
}
