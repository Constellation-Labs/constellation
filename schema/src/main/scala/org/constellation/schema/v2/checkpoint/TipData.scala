package org.constellation.schema.v2.checkpoint

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.constellation.schema.v2.Height

case class TipData(checkpointBlock: CheckpointBlock, numUses: Int, height: Height)

object TipData {
  implicit val tipDataEncoder: Encoder[TipData] = deriveEncoder
  implicit val tipDataDecoder: Decoder[TipData] = deriveDecoder
}
