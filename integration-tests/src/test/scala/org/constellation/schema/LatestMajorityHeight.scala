package org.constellation.schema

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class LatestMajorityHeight(lowest: Long, highest: Long)

object LatestMajorityHeight {
  implicit val latestMajorityHeightEncoder: Encoder[LatestMajorityHeight] = deriveEncoder
  implicit val latestMajorityHeightDecoder: Decoder[LatestMajorityHeight] = deriveDecoder
}
