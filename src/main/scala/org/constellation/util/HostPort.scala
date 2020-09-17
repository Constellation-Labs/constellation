package org.constellation.util

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}

case class HostPort(
  host: String,
  port: Int
)

object HostPort {
  implicit val hostPortEncoder: Encoder[HostPort] = deriveEncoder
  implicit val hostPortDecoder: Decoder[HostPort] = deriveDecoder
}
