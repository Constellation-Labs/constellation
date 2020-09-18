package org.constellation.schema.consensus

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class RoundId(id: String) extends AnyVal

object RoundId {
  implicit val roundIdEncoder: Encoder[RoundId] = deriveEncoder
  implicit val roundIdDecoder: Decoder[RoundId] = deriveDecoder
}
