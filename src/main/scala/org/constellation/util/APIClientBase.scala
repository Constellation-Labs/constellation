package org.constellation.util

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

case class HostPort(
  host: String,
  port: Int
)

object HostPort {
  implicit val hostPortEncoder: Encoder[HostPort] = deriveEncoder
  implicit val hostPortDecoder: Decoder[HostPort] = deriveDecoder
}
