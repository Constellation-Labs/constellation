package org.constellation.schema.signature

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.constellation.schema.Id
import org.constellation.schema.checkpoint.CheckpointBlock

case class SignatureRequest(checkpointBlock: CheckpointBlock, facilitators: Set[Id])

object SignatureRequest {
  implicit val signatureRequestEncoder: Encoder[SignatureRequest] = deriveEncoder
  implicit val signatureRequestDecoder: Decoder[SignatureRequest] = deriveDecoder
}
