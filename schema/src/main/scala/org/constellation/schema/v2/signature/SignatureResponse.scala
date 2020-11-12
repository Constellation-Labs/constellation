package org.constellation.schema.v2.signature

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class SignatureResponse(signature: Option[HashSignature], reRegister: Boolean = false)

object SignatureResponse {
  implicit val signatureResponseEncoder: Encoder[SignatureResponse] = deriveEncoder
  implicit val signatureResponseDecoder: Decoder[SignatureResponse] = deriveDecoder
}
