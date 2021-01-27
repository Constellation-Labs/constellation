package org.constellation.schema.snapshot

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class MajorityInfo(
  majorityRange: HeightRange,
  majorityGaps: List[HeightRange]
)

object MajorityInfo {
  implicit val encoder: Encoder[MajorityInfo] = deriveEncoder
  implicit val decoder: Decoder[MajorityInfo] = deriveDecoder
}
