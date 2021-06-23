package org.constellation.schema.snapshot

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

/**
  * Inclusive range of heights
  */
case class HeightRange(from: Long, to: Long) {
  def nonEmpty: Boolean = !empty

  def empty: Boolean = from > to
}

object HeightRange {
  val MaxRange = HeightRange(0, Long.MaxValue)
  val Empty = HeightRange(Long.MaxValue, 0)
  implicit val encoder: Encoder[HeightRange] = deriveEncoder
  implicit val decoder: Decoder[HeightRange] = deriveDecoder
}
