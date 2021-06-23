package org.constellation.schema.snapshot

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class FilterData(
  contents: Array[Byte],
  capacity: Long
)

object FilterData {
  implicit val encoder: Encoder[FilterData] = deriveEncoder
  implicit val decoder: Decoder[FilterData] = deriveDecoder
}
