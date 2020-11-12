package org.constellation.schema.v2

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.constellation.schema.ProtoAutoCodecs

case class Height(min: Long, max: Long) extends Ordered[Height] {
  override def compare(that: Height): Int = min.compare(that.min)
}

object Height extends ProtoAutoCodecs[org.constellation.schema.proto.Height, Height] {
  val cmp = org.constellation.schema.proto.Height

  implicit val heightEncoder: Encoder[Height] = deriveEncoder
  implicit val heightDecoder: Decoder[Height] = deriveDecoder
}
