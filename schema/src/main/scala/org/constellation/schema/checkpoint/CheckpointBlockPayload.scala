package org.constellation.schema.checkpoint

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.constellation.schema.Id
import org.constellation.schema.signature.{Signable, Signed}

import scala.language.implicitConversions

case class FinishedCheckpointBlock(checkpointCacheData: CheckpointCache, facilitators: Set[Id]) extends Signable

object FinishedCheckpointBlock {
  implicit val encoder: Encoder[FinishedCheckpointBlock] = deriveEncoder
  implicit val decoder: Decoder[FinishedCheckpointBlock] = deriveDecoder

  implicit def toFinishedCheckpoint(a: FinishedCheckpointBlock): FinishedCheckpoint =
    FinishedCheckpoint(a.checkpointCacheData, a.facilitators)
}

case class CheckpointBlockPayload(block: FinishedCheckpointBlock)

object CheckpointBlockPayload {
  implicit val encoder: Encoder[CheckpointBlockPayload] = deriveEncoder
  implicit val decoder: Decoder[CheckpointBlockPayload] = deriveDecoder
}
