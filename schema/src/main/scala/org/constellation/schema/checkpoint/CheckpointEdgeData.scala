package org.constellation.schema.checkpoint

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.signature.Signable

/**
  * Collection of references to transaction hashes
  *
  * @param hashes : TX edge hashes
  */
case class CheckpointEdgeData(
  hashes: Seq[String],
  messageHashes: Seq[String] = Seq(),
  observationsHashes: Seq[String]
) extends Signable

object CheckpointEdgeData {
  implicit val checkpointEdgeDataEncoder: Encoder[CheckpointEdgeData] = deriveEncoder
  implicit val checkpointEdgeDataDecoder: Decoder[CheckpointEdgeData] = deriveDecoder
}
