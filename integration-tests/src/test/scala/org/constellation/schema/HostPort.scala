package org.constellation.schema

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class HostPort(
  host: String,
  port: Int
)

object HostPort {
  implicit val hostPortEncoder: Encoder[HostPort] = deriveEncoder
  implicit val hostPortDecoder: Decoder[HostPort] = deriveDecoder
}
