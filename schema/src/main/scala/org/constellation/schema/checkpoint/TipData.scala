package org.constellation.schema.checkpoint

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.constellation.schema.Height

case class TipData(checkpointBlock: CheckpointBlock, numUses: Int, height: Height)

object TipData {
  implicit val tipDataEncoder: Encoder[TipData] = deriveEncoder
  implicit val tipDataDecoder: Decoder[TipData] = deriveDecoder
}
