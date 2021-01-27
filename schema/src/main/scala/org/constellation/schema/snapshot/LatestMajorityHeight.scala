package org.constellation.schema.snapshot

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class LatestMajorityHeight(lowest: Long, highest: Long)

object LatestMajorityHeight {
  def apply(heightRange: HeightRange): LatestMajorityHeight = new LatestMajorityHeight(heightRange.from, heightRange.to)
  implicit val latestMajorityHeightEncoder: Encoder[LatestMajorityHeight] = deriveEncoder
  implicit val latestMajorityHeightDecoder: Decoder[LatestMajorityHeight] = deriveDecoder
}
