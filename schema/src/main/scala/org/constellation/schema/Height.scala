package org.constellation.schema

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class Height(min: Long, max: Long) extends Ordered[Height] {
  override def compare(that: Height): Int = min.compare(that.min)
}

object Height {
  implicit val heightEncoder: Encoder[Height] = deriveEncoder
  implicit val heightDecoder: Decoder[Height] = deriveDecoder
}
