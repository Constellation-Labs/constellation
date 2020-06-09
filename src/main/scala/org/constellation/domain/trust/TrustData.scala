package org.constellation.domain.trust

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import org.constellation.schema.Id

case class TrustData(view: Map[Id, Double])

object TrustData {
  implicit val trustDataEncoder: Encoder[TrustData] = deriveEncoder
  implicit val trustDataDecoder: Decoder[TrustData] = deriveDecoder
}

case class TrustDataInternal(id: Id, view: Map[Id, Double])
