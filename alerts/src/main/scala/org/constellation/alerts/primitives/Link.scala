package org.constellation.alerts.primitives

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class Link (href: String, text: String)

object Link {
  implicit val encoder: Encoder[Link] = deriveEncoder
}