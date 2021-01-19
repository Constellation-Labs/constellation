package org.constellation.schema.snapshot

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class TotalSupply(height: Long, value: Long)

object TotalSupply {
  implicit val totalSupplyEncoder: Encoder[TotalSupply] = deriveEncoder
  implicit val totalSupplyDecoder: Decoder[TotalSupply] = deriveDecoder
}
