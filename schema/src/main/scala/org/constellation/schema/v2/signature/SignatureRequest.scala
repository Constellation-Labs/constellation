package org.constellation.schema.v2.signature

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.constellation.schema.v2.Id
import org.constellation.schema.v2.checkpoint.CheckpointBlock

case class SignatureRequest(checkpointBlock: CheckpointBlock, facilitators: Set[Id])

object SignatureRequest {
  implicit val signatureRequestEncoder: Encoder[SignatureRequest] = deriveEncoder
  implicit val signatureRequestDecoder: Decoder[SignatureRequest] = deriveDecoder
}
