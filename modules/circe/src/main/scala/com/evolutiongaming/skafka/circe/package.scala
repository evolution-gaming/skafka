package com.evolutiongaming.skafka

import io.circe.Encoder
import com.evolutiongaming.skafka.ToBytes
import com.evolutiongaming.catshelper.FromTry
import cats.syntax.contravariant.toContravariantOps
import io.circe.syntax.EncoderOps

package object circe {
  implicit def toBytesFromEncoder[F[_]: FromTry, A: Encoder]: ToBytes[F, A] = ToBytes.stringToBytes[F].contramap(_.asJson.noSpaces)
}
