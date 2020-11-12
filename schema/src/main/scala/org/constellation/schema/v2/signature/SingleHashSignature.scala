package org.constellation.schema.v2.signature

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class SingleHashSignature(hash: String, hashSignature: HashSignature) {

  def valid(expectedHash: String): Boolean =
    hash == expectedHash && hashSignature.valid(expectedHash)
}

object SingleHashSignature {
  implicit val singleHashSignatureEncoder: Encoder[SingleHashSignature] = deriveEncoder
  implicit val singleHashSignatureDecoder: Decoder[SingleHashSignature] = deriveDecoder
}
