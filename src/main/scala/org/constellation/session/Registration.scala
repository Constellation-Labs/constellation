package org.constellation.session

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.http4s.util.CaseInsensitiveString

object Registration {
  val `X-Id`: CaseInsensitiveString = CaseInsensitiveString("X-Id")

  case class JoinRequestPayload(host: String, port: Int, id: String)

  object JoinRequestPayload {
    implicit val joinRequestPayloadEncoder: Encoder[JoinRequestPayload] = deriveEncoder
    implicit val joinRequestPayloadDecoder: Decoder[JoinRequestPayload] = deriveDecoder
  }

}
