package org.constellation.schema.snapshot

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

/**
  * Inclusive range of heights
  */
case class HeightRange(from: Long, to: Long)

object HeightRange {
  val MaxRange = HeightRange(0, Long.MaxValue)
  val MinRange = HeightRange(0, 0)
  implicit val encoder: Encoder[HeightRange] = deriveEncoder
  implicit val decoder: Decoder[HeightRange] = deriveDecoder
}
